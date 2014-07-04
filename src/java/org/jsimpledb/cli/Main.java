
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
import java.util.LinkedHashSet;

import org.jsimpledb.cli.cmd.Command;
import org.jsimpledb.cli.func.Function;
import org.jsimpledb.core.Database;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.AbstractMain;

/**
 * CLI main entry point.
 */
public class Main extends AbstractMain {

    private File schemaFile;
    private final LinkedHashSet<Class<?>> addClasses = new LinkedHashSet<>();

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        if (option.equals("--schema")) {
            if (params.isEmpty())
                this.usageError();
            this.schemaFile = new File(params.removeFirst());
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

        // Start up KV database
        this.startupKVDatabase();

        // Set up console
        final Console console = new Console(new Database(this.kvdb), new FileInputStream(FileDescriptor.in), System.out);
        console.setHistoryFile(new File(new File(System.getProperty("user.home")), ".jsimpledb_history"));
        console.getSession().setReadOnly(this.readOnly);
        console.getSession().setVerbose(this.verbose);
        console.getSession().setSchemaVersion(this.schemaVersion);
        console.getSession().setAllowNewSchema(this.newSchema);
        if (this.schemaFile != null) {
            try {
                final InputStream input = new BufferedInputStream(new FileInputStream(this.schemaFile));
                try {
                    console.getSession().setSchemaModel(SchemaModel.fromXML(input));
                } finally {
                    input.close();
                }
            } catch (Exception e) {
                System.err.println(this.getName() + ": can't load schema from `" + this.schemaFile + "': " + e.getMessage());
                return 1;
            }
        }

        // Instantiate and add scanned classes
        for (Class<?> cl : this.addClasses) {
            Exception failure = null;
            Object obj = null;
            try {
                obj = cl.getConstructor(Session.class).newInstance(console.getSession());
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
                console.getSession().getCommands().put(command.getName(), command);
                //System.err.println(this.getName() + ": added command `" + command.getName() + "'");
            } else if (obj instanceof Function) {
                final Function function = (Function)obj;
                console.getSession().getFunctions().put(function.getName(), function);
                //System.err.println(this.getName() + ": added function `" + function.getName() + "'");
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
        System.err.println("  --add class       Add specified @CliCommand- or @CliFunction-annotated Java class");
        System.err.println("  --addpkg package  Scan for @CliCommand and @CliFunction classes under Java package");
        System.err.println("  --schema file     Load database schema from XML file");
        this.outputFlags();
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }
}

