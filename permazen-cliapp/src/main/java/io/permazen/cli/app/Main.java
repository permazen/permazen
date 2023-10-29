
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.app;

import com.google.common.base.Preconditions;

import io.permazen.Permazen;
import io.permazen.app.AbstractMain;
import io.permazen.cli.Console;
import io.permazen.cli.PermazenExec;
import io.permazen.cli.PermazenExecSession;
import io.permazen.cli.PermazenShell;
import io.permazen.cli.PermazenShellSession;
import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.core.Database;
import io.permazen.schema.SchemaModel;
import io.permazen.PermazenFactory;
import io.permazen.core.Database;
import io.permazen.core.encoding.Encoding;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.spring.PermazenClassScanner;
import io.permazen.spring.PermazenEncodingScanner;
import io.permazen.util.ApplicationClassLoader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import org.apache.common.cli.CommandLine;
import org.apache.common.cli.CommandLineParser;
import org.apache.common.cli.Option;
import org.apache.common.cli.Options;
import org.dellroad.jct.core.ConsoleSession;
import org.dellroad.jct.core.ExecSession;
import org.dellroad.jct.core.ShellRequest;
import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.core.simple.CommandBundle;
import org.dellroad.jct.core.simple.SimpleCommandSupport;
import org.dellroad.jct.core.simple.SimpleExec;
import org.dellroad.jct.core.simple.SimpleExecRequest;
import org.dellroad.jct.core.simple.SimpleShell;
import org.dellroad.jct.core.simple.SimpleShellRequest;
import org.dellroad.jct.core.simple.command.HelpCommand;
import org.dellroad.jct.core.util.ConsoleUtil;
import org.dellroad.jct.jshell.JShellShell;
import org.dellroad.jct.jshell.JShellShellSession;
import org.dellroad.jct.jshell.command.JShellCommand;
import org.dellroad.jct.ssh.simple.SimpleConsoleSshServer;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * Permazen CLI application main entry point.
 */
public class Main {

    public static final String HISTORY_FILE = ".permazen_history";

    private final List<CommandBundle> commandBundles = CommandBundle.scanAndGenerate().collect(Collectors.toList());
    private File schemaFile;
    private File historyFile = new File(new File(System.getProperty("user.home")), HISTORY_FILE);
    private SessionMode mode = SessionMode.PERMAZEN;
    private boolean readOnly;

    protected final ConsoleExec exec;
    protected final ConsoleShell shell;

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        switch (option) {
        case "--schema-file":
            if (params.isEmpty())
                this.usageError();
            this.schemaFile = new File(params.removeFirst());
            break;
        case "--history-file":
            if (params.isEmpty())
                this.usageError();
            this.historyFile = new File(params.removeFirst());
            break;
        case "--read-only":
            this.readOnly = true;
            break;
        case "--core-mode":
            this.mode = SessionMode.CORE_API;
            break;
        case "--kv-mode":
            this.mode = SessionMode.KEY_VALUE;
            break;
        default:
            return false;
        }
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {

        // Parse command line
        final ArrayDeque<String> params = new ArrayDeque<>(Arrays.asList(args));
        final int result = this.parseOptions(params);
        if (result != -1)
            return result;

        // Read schema file from `--schema-file' (if any)
        SchemaModel schemaModel = null;
        if (this.schemaFile != null) {
            try {
                try (InputStream input = new BufferedInputStream(new FileInputStream(this.schemaFile))) {
                    schemaModel = SchemaModel.fromXML(input);
                }
            } catch (Exception e) {
                System.err.println(this.getName() + ": can't load schema from \"" + this.schemaFile + "\": " + e.getMessage());
                if (this.verbose)
                    e.printStackTrace(System.err);
                return 1;
            }
        }

        // Set up Database
        final Database db = this.startupKVDatabase();

        // Load Permazen layer, if specified
        final Permazen jdb = this.schemaClasses != null ? this.getPermazenFactory(db).newPermazen() : null;

        // Sanity check consistent schema model if both --schema-file and --model-pkg were specified
        if (jdb != null) {
            if (schemaModel != null) {
                if (!schemaModel.equals(jdb.getSchemaModel())) {
                    System.err.println(this.getName() + ": schema from \"" + this.schemaFile + "\" conflicts with schema generated"
                      + " from scanned classes");
                    System.err.println(schemaModel.differencesFrom(jdb.getSchemaModel()));
                    return 1;
                }
            } else
                schemaModel = jdb.getSchemaModel();
        }

        // Downgrade to Core API mode from Permazen mode if no Java model classes provided
        if (jdb == null && this.mode.equals(SessionMode.PERMAZEN)) {
            System.err.println(this.getName() + ": entering core API mode because no Java model classes were specified");
            this.mode = SessionMode.CORE_API;
        }

        // Create and configure console components
        switch (this.mode) {
        case KEY_VALUE:
            this.exec = this.createConsoleExec(db.getKVDatabase(), null, null);
            this.shell = this.createConsoleShell(db.getKVDatabase(), null, null);
            break;
        case CORE_API:
            this.exec = this.createConsoleExec(null, db, null);
            this.shell = this.createConsoleShell(null, db, null);
            break;
        case PERMAZEN:
            this.exec = this.createConsoleExec(null, null, jdb);
            this.shell = this.createConsoleShell(null, null, jdb);
            break;
        default:
            throw new RuntimeException("unexpected case");
        }
        exec.getCommandBundles().addAll(this.commandBundles);
        shell.getCommandBundles().addAll(this.commandBundles);


        // Handle exec mode
        if (!params.isEmpty()) {

            // Handle file input
            for (String filename : this.execFiles) {
                final File file = new File(filename);
                try (InputStream input = new FileInputStream(file)) {
                    if (!this.parseAndExecuteCommands(console, new InputStreamReader(input), file.getName()))
                        return 1;
                } catch (IOException e) {
                    session.getError().println("Error: error opening " + file.getName() + ": " + e);
                    return 1;
                }
            }

            // Handle command-line commands
            for (String command : this.execCommands) {
                if (!this.parseAndExecuteCommands(console, new StringReader(command), null))
                    return 1;
            }

            // Handle standard input
            if (!this.keyboardInput) {
                if (!this.parseAndExecuteCommands(console, new InputStreamReader(System.in), "(stdin)"))
                    return 1;
            }

            // Run console if not in batch mode
            if (this.keyboardInput && !this.batchMode)
                console.run();

            // Shut down KV database
            this.shutdownKVDatabase();

            // Done
            return 0;
        }
    }

    private boolean parseAndExecuteCommands(Console console, Reader input, String inputDescription) {
        try {
            return console.runNonInteractive(input, inputDescription);
        } catch (IOException e) {
            console.getSession().getWriter().println("Error: error reading " + inputDescription + ": " + e);
            return false;
        }
    }

    @Override
    protected String getName() {
        return "permazen";
    }

    @Override
    protected void usageMessage() {
        System.err.println("Usage:");
        System.err.println("  " + this.getName() + " [options] [command ...]");
        System.err.println("Options:");
        this.outputFlags(new String[][] {
          { "--history-file file",      "Specify file for CLI command history (default ~/" + HISTORY_FILE + ")" },
          { "--schema-file file",       "Load core database schema from XML file" },
          { "--core-mode",              "Force core API mode (default if neither Java model classes nor schema are provided)" },
          { "--kv-mode",                "Force key/value mode" },
          { "--read-only",              "Read-only mode" },
        });
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).contains("win");
    }

// Subclass Hooks

    protected ConsoleExec createConsoleExec(KVDatabase kvdb, Database db, Permazen jdb) {
        return new ConsoleExec(kvdb, db, jdb);
    }

    protected ConsoleShell createConsoleShell(KVDatabase kvdb) {
        return new ConsoleShell(kvdb, db, jdb);
    }

    protected Session createSession(ConsoleSession<?, ?> session) {
        final Session session = new Session(session, this.jdb, this.db, this.kvdb);
        session.setDatabaseDescription(this.getDatabaseDescription());
        session.setReadOnly(this.readOnly);
        session.setVerbose(this.verbose);
        session.setSchemaModel(this.schemaModel);
        session.setSchemaVersion(this.schemaVersion);
        session.setAllowNewSchema(this.allowNewSchema);
        session.loadCommandsFromClasspath();
        return session;
    }

        // Create and configure console components
        switch (this.mode) {
        case KEY_VALUE:
            exec = new PermazenExec(db.getKVDatabase());
            shell = new PermazenShell(db.getKVDatabase());
            break;
        case CORE_API:
            exec = new PermazenExec(db);
            shell = new PermazenShell(db);
            break;
        case PERMAZEN:
            exec = new PermazenExec(jdb);
            shell = new PermazenShell(jdb);
            break;
        default:
            throw new RuntimeException("unexpected case");
        }
        exec.getCommandBundles().addAll(this.commandBundles);
        shell.getCommandBundles().addAll(this.commandBundles);


// ConsoleExec

    protected class ConsoleExec extends PermazenExec {

        protected ConsoleExec(KVDatabase kvdb, Database db, Permazen jdb {
            super(kvdb, db, jdb);
        }

        @Override
        protected PermazenExecSession createPermazenExecSession(ExecRequest request, FoundCommand command) throws IOException {
            return new PermazenExecSession(this, request, command);
        }

        @Override
        protected io.permazen.cli.Session createSession(PermazenExecSession execSession) {
            return new io.permazen.cli.Session(execSession, this.jdb, this.db, this.kvdb);
        }
    }

// ConsoleShell

    protected io.permazen.cli.Session createSession(PermazenShellSession shellSession) {
        return new io.permazen.cli.Session(shellSession, this.jdb, this.db, this.kvdb);
    }
}
