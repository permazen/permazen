
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
import org.jsimpledb.cli.cmd.Command;
import org.jsimpledb.core.Database;
import org.jsimpledb.parse.func.Function;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.spring.AnnotatedClassScanner;
import org.jsimpledb.util.AbstractMain;

/**
 * CLI main entry point.
 */
public class Main extends AbstractMain {

    public static final String HISTORY_FILE = ".jsimpledb_history";

    private File schemaFile;
    private boolean coreMode;
    private final LinkedHashSet<Class<?>> commandClasses = new LinkedHashSet<>();
    private final LinkedHashSet<Class<?>> functionClasses = new LinkedHashSet<>();

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        if (option.equals("--schema-file")) {
            if (params.isEmpty())
                this.usageError();
            this.schemaFile = new File(params.removeFirst());
            this.allowAutoDemo = false;
        } else if (option.equals("--core-mode"))
            this.coreMode = true;
        else if (option.equals("--cmd-pkg")) {
            if (params.isEmpty())
                this.usageError();
            this.scanCommandClasses(params.removeFirst());
        } else if (option.equals("--func-pkg")) {
            if (params.isEmpty())
                this.usageError();
            this.scanFunctionClasses(params.removeFirst());
        } else
            return false;
        return true;
    }

    private void scanCommandClasses(String pkgname) {
        for (String className : new AnnotatedClassScanner(Command.class).scanForClasses(pkgname.split("[\\s,]")))
            this.commandClasses.add(this.loadClass(className));
    }

    private void scanFunctionClasses(String pkgname) {
        for (String className : new AnnotatedClassScanner(Function.class).scanForClasses(pkgname.split("[\\s,]")))
            this.functionClasses.add(this.loadClass(className));
    }

    @Override
    public int run(String[] args) throws Exception {

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

        // Set up Database
        final Database db = this.startupKVDatabase();

        // Load JSimpleDB layer, if specified
        final JSimpleDB jdb = this.schemaClasses != null ? this.getJSimpleDBFactory(db).newJSimpleDB() : null;

        // Sanity check consistent schema model if both --schema-file and --model-pkg were specified
        if (jdb != null) {
            if (schemaModel != null) {
                if (!schemaModel.equals(jdb.getSchemaModel())) {
                    System.err.println(this.getName() + ": schema from `" + this.schemaFile + "' conflicts with schema generated"
                      + " from scanned classes");
                    System.err.println(schemaModel.differencesFrom(jdb.getSchemaModel()));
                    return 1;
                }
            } else
                schemaModel = jdb.getSchemaModel();
        }

        // Core API mode or JSimpleDB mode?
        this.coreMode |= jdb == null;

        // Perform test transaction
        if (this.coreMode)
            this.performTestTransaction(db, schemaModel);
        else
            this.performTestTransaction(jdb);

        // Set up console
        final Console console = this.coreMode ?
          new Console(db, new FileInputStream(FileDescriptor.in), System.out) :
          new Console(jdb, new FileInputStream(FileDescriptor.in), System.out);
        console.setHistoryFile(new File(new File(System.getProperty("user.home")), HISTORY_FILE));

        // Set up CLI session
        final CliSession session = console.getSession();
        session.setDatabaseDescription(this.getDatabaseDescription());
        session.setReadOnly(this.readOnly);
        session.setVerbose(this.verbose);
        session.setSchemaModel(schemaModel);
        session.setSchemaVersion(this.schemaVersion);
        session.setAllowNewSchema(this.allowNewSchema);
        session.registerStandardFunctions();
        session.registerStandardCommands();
        try {
            for (Class<?> cl : this.commandClasses) {
                final Command annotation = cl.getAnnotation(Command.class);
                if (!this.coreMode || annotation.worksInCoreAPIMode())
                    session.registerCommand(cl);
            }
            for (Class<?> cl : this.functionClasses) {
                final Function annotation = cl.getAnnotation(Function.class);
                if (!this.coreMode || annotation.worksInCoreAPIMode())
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
          { "--cmd-pkg package",    "Register @Command-annotated classes found under the specified Java package" },
          { "--func-pkg package",   "Register @Function-annotated classes found under the specified Java package" },
          { "--core-mode",          "Force core API mode even though Java model classes are provided" },
        });
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }
}

