
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.app;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.SessionMode;
import org.jsimpledb.app.AbstractMain;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.cli.Console;
import org.jsimpledb.core.Database;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.ParseContext;

/**
 * CLI main entry point.
 */
public class Main extends AbstractMain {

    public static final String HISTORY_FILE = ".jsimpledb_history";

    private File schemaFile;
    private SessionMode mode = SessionMode.JSIMPLEDB;
    private final ArrayList<String> oneShotCommands = new ArrayList<>();

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        if (option.equals("--schema-file")) {
            if (params.isEmpty())
                this.usageError();
            this.schemaFile = new File(params.removeFirst());
        } else if (option.equals("--command") || option.equals("-c")) {
            if (params.isEmpty())
                this.usageError();
            this.oneShotCommands.add(params.removeFirst());
        } else if (option.equals("--core-mode"))
            this.mode = SessionMode.CORE_API;
        else if (option.equals("--kv-mode"))
            this.mode = SessionMode.KEY_VALUE;
        else
            return false;
        return true;
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

        // Downgrade to Core API mode from JSimpleDB mode if no Java model classes provided
        if (jdb == null && this.mode.equals(SessionMode.JSIMPLEDB)) {
            System.err.println(this.getName() + ": entering core API mode because no Java model classes were specified");
            this.mode = SessionMode.CORE_API;
        }

        // Set up console
        final Console console;
        switch (this.mode) {
        case KEY_VALUE:
            console = new Console(db.getKVDatabase(), new FileInputStream(FileDescriptor.in), System.out);
            break;
        case CORE_API:
            console = new Console(db, new FileInputStream(FileDescriptor.in), System.out);
            break;
        case JSIMPLEDB:
            console = new Console(jdb, new FileInputStream(FileDescriptor.in), System.out);
            break;
        default:
            console = null;
            assert false;
            break;
        }
        console.setHistoryFile(new File(new File(System.getProperty("user.home")), HISTORY_FILE));

        // Set up CLI session
        final CliSession session = console.getSession();
        session.setDatabaseDescription(this.getDatabaseDescription());
        session.setReadOnly(this.readOnly);
        session.setVerbose(this.verbose);
        session.setSchemaModel(schemaModel);
        session.setSchemaVersion(this.schemaVersion);
        session.setAllowNewSchema(this.allowNewSchema);
        session.loadFunctionsFromClasspath();
        session.loadCommandsFromClasspath();

        // Handle one-shot command mode
        if (!this.oneShotCommands.isEmpty()) {
            for (String text : this.oneShotCommands) {

                // Parse command(s)
                final List<CliSession.Action> actions = console.parseCommand(text);
                if (actions == null) {
                    session.getWriter().println("Error: failed to parse command `" + ParseContext.truncate(text, 40) + "'");
                    return 1;
                }

                // Execute command(s)
                for (CliSession.Action action : console.parseCommand(text)) {
                    if (!session.performCliSessionAction(action))
                        return 1;
                }
            }
            return 0;
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
          { "--schema-file file",       "Load core database schema from XML file" },
          { "--core-mode",              "Force core API mode (default if neither Java model classes nor schema are provided)" },
          { "--kv-mode",                "Force key/value mode" },
          { "--command, -c command",    "Execute the given command and then exit (may be repeated)" },
        });
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }
}
