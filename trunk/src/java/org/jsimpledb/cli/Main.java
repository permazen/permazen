
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.cli.cmd.Command;
import org.jsimpledb.cli.func.Function;
import org.jsimpledb.core.Database;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.spring.ScanClassPathClassScanner;
import org.jsimpledb.util.AbstractMain;

/**
 * CLI main entry point.
 */
public class Main extends AbstractMain {

    private File schemaFile;
    private final LinkedHashSet<Class<?>> addClasses = new LinkedHashSet<>();
    private HashSet<Class<?>> schemaClasses;

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        if (option.equals("--schema-file")) {
            if (params.isEmpty())
                this.usageError();
            this.schemaFile = new File(params.removeFirst());
        } else if (option.equals("--schema-pkg")) {
            if (params.isEmpty())
                this.usageError();
            this.scanSchemaClasses(params.removeFirst());
        } else if (option.equals("--add")) {
            if (params.isEmpty())
                this.usageError();
            this.addClass(params.removeFirst());
        } else if (option.equals("--addpkg")) {
            if (params.isEmpty())
                this.usageError();
            this.scanClasses(params.removeFirst());
        } else
            return false;
        return true;
    }

    private void scanClasses(String pkgname) {
        for (String className : new ClassPathScanner().scanForClasses(pkgname.split("[\\s,]")))
            this.addClass(className);
    }

    private void addClass(String className) {
        try {
            this.addClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("failed to load class `" + className + "'", e);
        }
    }

    private void scanSchemaClasses(String pkgname) {
        if (this.schemaClasses == null)
            this.schemaClasses = new HashSet<>();
        for (String className : new ScanClassPathClassScanner().scanForClasses(pkgname.split("[\\s,]"))) {
            try {
                schemaClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("failed to load class `" + className + "'", e);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // Register normal commands and functions
        this.scanClasses(Command.class.getPackage().getName());
        this.scanClasses(Function.class.getPackage().getName());

        // Parse command line
        final ArrayDeque<String> params = new ArrayDeque<String>(Arrays.asList(args));
        final int result = this.parseOptions(params);
        if (result != -1)
            return result;
        switch (params.size()) {
        case 0:
            break;
        default:
            this.usageError();
            return 1;
        }

        // Read schema file from `--schema-file' (if any)
        SchemaModel schemaModel = null;
        if (this.schemaFile != null) {
            try {
                final InputStream input = new BufferedInputStream(new FileInputStream(this.schemaFile));
                try {
                    schemaModel = SchemaModel.fromXML(input);
                } finally {
                    input.close();
                }
            } catch (Exception e) {
                System.err.println(this.getName() + ": can't load schema from `" + this.schemaFile + "': " + e.getMessage());
                return 1;
            }
        }

        // Start up KV database
        this.startupKVDatabase();
        final Database db = new Database(this.kvdb);

        // Load JSimpleDB layer, if specified
        final JSimpleDB jdb = this.schemaClasses != null ? new JSimpleDB(db, this.schemaVersion, this.schemaClasses) : null;

        // Sanity check consistent schema model if both --schema-file and --schema-pkg were specified
        if (jdb != null && schemaModel != null) {
            if (!schemaModel.equals(jdb.getSchemaModel())) {
                System.err.println(this.getName() + ": schema from `" + this.schemaFile + "' conflicts with schema generated"
                  + " from scanned classes " + this.schemaClasses);
                return 1;
            }
        }

        // Set up console
        final Console console = jdb != null ?
          new Console(jdb, new FileInputStream(FileDescriptor.in), System.out) :
          new Console(db, new FileInputStream(FileDescriptor.in), System.out);
        final Session session = console.getSession();
        console.setHistoryFile(new File(new File(System.getProperty("user.home")), ".jsimpledb_history"));
        session.setReadOnly(this.readOnly);
        session.setVerbose(this.verbose);
        session.setSchemaModel(schemaModel);
        session.setSchemaVersion(this.schemaVersion);
        session.setAllowNewSchema(this.newSchema);

        // Instantiate and add scanned CLI classes
        for (Class<?> cl : this.addClasses) {
            Exception failure = null;
            Object obj = null;
            try {
                obj = cl.getConstructor(Session.class).newInstance(session);
            } catch (NoSuchMethodException e) {
                try {
                    obj = cl.getConstructor().newInstance();
                } catch (NoSuchMethodException e2) {
                    System.err.println(this.getName() + ": can't find suitable constructor in class " + cl.getName());
                    return 1;
                } catch (Exception e2) {
                    failure = e2;
                }
            } catch (Exception e) {
                failure = e;
            }
            if (failure != null)
                System.err.println(this.getName() + ": can't instantiate class " + cl.getName() + ": " + failure);
            if (obj instanceof Command) {
                final Command command = (Command)obj;
                session.getCommands().put(command.getName(), command);
            } else if (obj instanceof Function) {
                final Function function = (Function)obj;
                session.getFunctions().put(function.getName(), function);
            } else {
                System.err.println(this.getName() + ": warning: class "
                  + cl.getName() + " is neither a command nor a function; ignoring");
            }
        }

        // Run console
        console.run();

        // Shut down KV database
        this.shutdownKVDatabase();

        // Done
        return 0;
    }

    @Override
    protected String getName() {
        return "jsimpledb";
    }

    @Override
    protected void usageMessage() {
        System.err.println("Usage:");
        System.err.println("  " + this.getName() + " [options]");
        System.err.println("Options:");
        this.outputFlags(new String[][] {
          { "--add class",          "Add specified @CliCommand- or @CliFunction-annotated Java class" },
          { "--addpkg package",     "Scan for @CliCommand and @CliFunction classes under Java package" },
          { "--schema-file file",   "Load core database schema from XML file" },
          { "--schema-pkg package", "Scan for @JSimpleClass classes under Java package to build schema" },
        });
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }
}

