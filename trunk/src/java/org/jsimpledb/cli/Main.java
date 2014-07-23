
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

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.cli.cmd.CliCommand;
import org.jsimpledb.cli.cmd.Command;
import org.jsimpledb.cli.func.CliFunction;
import org.jsimpledb.cli.func.Function;
import org.jsimpledb.core.Database;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.spring.AnnotatedClassScanner;
import org.jsimpledb.util.AbstractMain;

/**
 * CLI main entry point.
 */
public class Main extends AbstractMain {

    private File schemaFile;
    private final LinkedHashSet<Class<?>> commandClasses = new LinkedHashSet<>();
    private final LinkedHashSet<Class<?>> functionClasses = new LinkedHashSet<>();

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        if (option.equals("--schema-file")) {
            if (params.isEmpty())
                this.usageError();
            this.schemaFile = new File(params.removeFirst());
        } else if (option.equals("--cmdpkg")) {
            if (params.isEmpty())
                this.usageError();
            this.scanCommandClasses(params.removeFirst());
        } else if (option.equals("--funcpkg")) {
            if (params.isEmpty())
                this.usageError();
            this.scanFunctionClasses(params.removeFirst());
        } else
            return false;
        return true;
    }

    private void scanCommandClasses(String pkgname) {
        for (String className : new AnnotatedClassScanner(CliCommand.class).scanForClasses(pkgname.split("[\\s,]")))
            this.commandClasses.add(this.loadClass(className));
    }

    private void scanFunctionClasses(String pkgname) {
        for (String className : new AnnotatedClassScanner(CliFunction.class).scanForClasses(pkgname.split("[\\s,]")))
            this.functionClasses.add(this.loadClass(className));
    }

    @Override
    public int run(String[] args) throws Exception {

        // Register built-in commands and functions
        this.scanCommandClasses(Command.class.getPackage().getName());
        this.scanFunctionClasses(Function.class.getPackage().getName());

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
                if (this.verbose)
                    e.printStackTrace(System.err);
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
        session.setAllowNewSchema(this.allowNewSchema);
        try {
            for (Class<?> cl : this.commandClasses) {
                final CliCommand annotation = cl.getAnnotation(CliCommand.class);
                if (jdb != null ? annotation.worksInJSimpleDBMode() : annotation.worksInCoreAPIMode())
                    session.registerCommand(cl);
            }
            for (Class<?> cl : this.functionClasses) {
                final CliFunction annotation = cl.getAnnotation(CliFunction.class);
                if (jdb != null ? annotation.worksInJSimpleDBMode() : annotation.worksInCoreAPIMode())
                    session.registerFunction(cl);
            }
        } catch (IllegalArgumentException e) {
            System.err.println(this.getName() + ": " + e.getMessage());
            if (this.verbose)
                e.printStackTrace(System.err);
            return 1;
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
          { "--schema-file file",   "Load core database schema from XML file" },
          { "--cmdpkg package",     "Register @CliCommand-annotated classes found under the specified Java package" },
          { "--funcpkg package",     "Register @CliFunction-annotated classes found under the specified Java package" },
        });
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }
}

