
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.config;

import com.google.common.base.Preconditions;

import io.permazen.Permazen;
import io.permazen.cli.PermazenExec;
import io.permazen.cli.PermazenExecSession;
import io.permazen.cli.PermazenShell;
import io.permazen.cli.PermazenShellSession;
import io.permazen.cli.Session;
import io.permazen.core.Database;
import io.permazen.kv.KVDatabase;
import io.permazen.util.ApplicationClassLoader;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for a CLI application.
 *
 * <p>
 * To use this class (or a subclass), first invoke {@link #startup startup()} to configure and start the database
 * using the command-line flags that were given. Then, access the database to create CLI sessions as needed.
 * When done, invoke {@link #shutdownDatabase shutdownDatabase()} to stop.
 */
public abstract class CliConfig {

    private static final int MIN_HELP_COLUMNS = 80;
    private static final int DEFAULT_HELP_COLUMNS = 132;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Generic options
    protected OptionSpec<Void> readOnlyOption;
    protected OptionSpec<Void> verboseOption;
    protected OptionSpec<Void> helpOption;

    // Other info
    protected final ApplicationClassLoader loader = ApplicationClassLoader.getInstance();

    // Internal state
    protected boolean setReadOnly;
    protected boolean setVerbose;

// Workflow

    /**
     * Configure and startup a {@link Session}.
     *
     * <p>
     * If an error occurs, or {@code --help} is requested, null will be returned and
     * an appropriate message printed to the console.
     *
     * @param out standard output
     * @param err standard error
     * @param numColumns number of columns in terminal display, or -1 if unknown
     * @param params command line parameters
     * @return true if successful, or false if the {@code --help} flag was given
     * @throws IllegalArgumentException if startup failed due to misconfiguration
     * @throws IllegalArgumentException if any parameter is null
     */
    public boolean startup(PrintStream out, PrintStream err, int numColumns, String[] params) {

        // Sanity check
        Preconditions.checkArgument(out != null, "null out");
        Preconditions.checkArgument(err != null, "null err");
        Preconditions.checkArgument(params != null, "null params");

        // Create option parser
        final OptionParser parser = this.createOptionParser(numColumns);

        // Configure available options
        this.addOptions(parser);
        try {

            // Parse options
            final OptionSet options = parser.parse(params);
            if (this.helpOption != null && options.has(this.helpOption)) {
                this.showHelp(out, parser);
                return false;
            }

            // Review/process options
            this.processOptions(options);

            // Startup database
            this.startupDatabase(options);
        } catch (OptionException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }

        // Done
        return true;
    }

// Options

    /**
     * Create the {@link OptionParser} to be used to parse command line options.
     *
     * @param numColumns number of columns in terminal display, or -1 if unknown
     * @return option parser
     */
    protected OptionParser createOptionParser(int numColumns) {
        final OptionParser parser = new OptionParser(false);
        if (numColumns == -1)
            numColumns = DEFAULT_HELP_COLUMNS;
        numColumns = Math.max(MIN_HELP_COLUMNS, numColumns);
        parser.formatHelpWith(new BuiltinHelpFormatter(numColumns, 2) {
            @Override
            protected void appendTypeIndicator(StringBuilder buf, String type, String description, char start, char end) {
                super.appendTypeIndicator(buf, null, description, start, end);
            }
        });
        return parser;
    }

    /**
     * Configure an {@link OptionParser} with the comand line flags supported by this instance.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     */
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        this.addGenericOptions(parser);
    }

    /**
     * Add generic command line flags to the given option parser.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     * @throws IllegalStateException if an option being added has already been added
     */
    protected void addGenericOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.readOnlyOption == null, "duplicate option");
        Preconditions.checkState(this.verboseOption == null, "duplicate option");
        Preconditions.checkState(this.helpOption == null, "duplicate option");
        this.readOnlyOption = parser.accepts("read-only", "Create read-only transactions");
        this.verboseOption = parser.acceptsAll(List.of("v", "verbose"), "Show verbose error messages");
        this.helpOption = parser.acceptsAll(List.of("h", "help"), "Show help message").forHelp();
    }

    /**
     * Review parsed options and do any preprocessing before starting database.
     *
     * @param options parsed command line options
     * @throws OptionException if some option(s) are invalid
     * @throws IllegalArgumentException if some option(s) are invalid
     * @throws IllegalArgumentException if {@code options} is null
     */
    protected void processOptions(OptionSet options) {
        this.setReadOnly = this.readOnlyOption != null && options.has(this.readOnlyOption);
        this.setVerbose = this.verboseOption != null && options.has(this.verboseOption);
    }

    /**
     * Print the help message.
     *
     * @param out output for help message
     * @param parser command line flag parser
     * @throws IllegalArgumentException if either parameter is null
     */
    public void showHelp(PrintStream out, OptionParser parser) {
        Preconditions.checkArgument(out != null, "null out");
        Preconditions.checkArgument(parser != null, "null parwser");
        try {
            parser.printHelpOn(out);
        } catch (IOException e) {
            out.println("[Error printing help]: " + e);
        }
    }

// Database

    /**
     * Configure and start up the database based on the parsed command line options.
     *
     * @param options parsed command line options
     * @throws OptionException if some option(s) are invalid
     * @throws IllegalArgumentException if some option(s) are invalid
     * @throws IllegalArgumentException if {@code options} is null
     * @throws IllegalStateException if already started
     */
    public abstract void startupDatabase(OptionSet options);

    /**
     * Shutdown the database.
     */
    public abstract void shutdownDatabase();

    public abstract String getDatabaseDescription();

    public KVDatabase getKVDatabase() {
        return null;
    }

    public Database getDatabase() {
        return null;
    }

    public Permazen getPermazen() {
        return null;
    }

// Session

    /**
     * Configure a new {@link Session}.
     *
     * @param session new session to configure
     * @throws IllegalArgumentException if {@code session} is null
     */
    protected void configureSession(Session session) {
        session.setDatabaseDescription(this.getDatabaseDescription());
        if (this.setReadOnly)
            session.setReadOnly(true);
        if (this.setVerbose)
            session.setVerbose(true);
    }

// Console

    /**
     * Create a new {@link PermazenExec}.
     *
     * @throws IllegalStateException if not started yet
     */
    public PermazenExec newPermazenExec() {
        final Permazen jdb = this.getPermazen();
        final Database db = jdb == null ? this.getDatabase() : null;
        final KVDatabase kvdb = jdb == null && db == null ? this.getKVDatabase() : null;
        Preconditions.checkState(jdb != null || db != null || kvdb != null, "database not started");
        return new PermazenExec(kvdb, db, jdb) {
            @Override
            protected io.permazen.cli.Session createSession(PermazenExecSession execSession) {
                final io.permazen.cli.Session session = super.createSession(execSession);
                CliConfig.this.configureSession(session);
                return session;
            }
        };
    }

    /**
     * Create a new {@link PermazenShell}.
     *
     * @throws IllegalStateException if not started yet
     */
    public PermazenShell newPermazenShell() {
        final Permazen jdb = this.getPermazen();
        final Database db = jdb == null ? this.getDatabase() : null;
        final KVDatabase kvdb = jdb == null && db == null ? this.getKVDatabase() : null;
        Preconditions.checkState(jdb != null || db != null || kvdb != null, "database not started");
        return new PermazenShell(kvdb, db, jdb) {
            @Override
            protected io.permazen.cli.Session createSession(PermazenShellSession shellSession) {
                final io.permazen.cli.Session session = super.createSession(shellSession);
                CliConfig.this.configureSession(session);
                return session;
            }
        };
    }

// Subclass Methods

    /**
     * Load a class.
     *
     * @param type expected type
     * @param className class name
     * @return class with name {@code className}
     * @throws IllegalArgumentException if class can't be loaded or has the wrong type
     * @throws IllegalArgumentException if either parameter is null
     */
    protected <T> Class<? extends T> loadClass(Class<T> type, String className) {
        Preconditions.checkArgument(type != null, "null type");
        Preconditions.checkArgument(className != null, "null className");
        try {
            final Class<?> cl = Class.forName(className, false, this.loader);
            try {
                return cl.asSubclass(type);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(String.format("not a sub-type of %s", type.getName()));
            }
        } catch (ClassCastException | ClassNotFoundException | LinkageError e) {
            throw new IllegalArgumentException(String.format("invalid class \"%s\": %s", className, e.getMessage()), e);
        }
    }

    /**
     * Load and instantiate a class.
     *
     * @param type expected type
     * @param className class name
     * @return new instance of class
     * @throws IllegalArgumentException if class can't be loaded, instantiated, or has the wrong type
     * @throws IllegalArgumentException if either parameter is null
     */
    protected <T> T instantiateClass(Class<T> type, String className) {
        try {
            return this.loadClass(type, className).getConstructor().newInstance();
        } catch (ReflectiveOperationException | ExceptionInInitializerError e) {
            throw new IllegalArgumentException(String.format("invalid class \"%s\": %s", className, e.getMessage()), e);
        }
    }
}
