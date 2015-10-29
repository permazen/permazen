
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.util.AddPrefixFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.Terminal;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.Completer;
import jline.console.history.FileHistory;

/**
 * CLI console.
 */
public class Console {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final ConsoleReader console;
    protected final CliSession session;

    private final CommandParser commandParser = new CommandParser();
    private final CommandListParser commandListParser = new CommandListParser(this.commandParser);

    private FileHistory history;

    /**
     * Simplified constructor for {@link org.jsimpledb.SessionMode#KEY_VALUE} mode.
     *
     * @param kvdb key/value {@link KVDatabase}
     * @param input console input
     * @param output console output
     * @throws IOException if an I/O error occurs
     */
    public Console(KVDatabase kvdb, InputStream input, OutputStream output) throws IOException {
        this(kvdb, null, null, input, output, null, null, null);
    }

    /**
     * Simplified constructor for {@link org.jsimpledb.SessionMode#CORE_API} mode.
     *
     * @param db core API {@link Database}
     * @param input console input
     * @param output console output
     * @throws IOException if an I/O error occurs
     */
    public Console(Database db, InputStream input, OutputStream output) throws IOException {
        this(null, db, null, input, output, null, null, null);
    }

    /**
     * Simplified constructor for {@link org.jsimpledb.SessionMode#JSIMPLEDB} mode.
     *
     * @param jdb {@link JSimpleDB} database
     * @param input console input
     * @param output console output
     * @throws IOException if an I/O error occurs
     */
    public Console(JSimpleDB jdb, InputStream input, OutputStream output) throws IOException {
        this(null, null, jdb, input, output, null, null, null);
    }

    /**
     * Generic constructor.
     *
     * @param kvdb {@link KVDatabase} for {@link org.jsimpledb.SessionMode#KEY_VALUE} (otherwise must be null)
     * @param db {@link Database} for {@link org.jsimpledb.SessionMode#CORE_API} (otherwise must be null)
     * @param jdb {@link JSimpleDB} for {@link org.jsimpledb.SessionMode#JSIMPLEDB} (otherwise must be null)
     * @param input console input
     * @param output console output
     * @param terminal JLine terminal interface, or null for default
     * @param encoding character encoding for {@code terminal}, or null for default
     * @param appName JLine application name, or null for none
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if not exactly one of {@code kvdb}, {@code db} or {@code jdb} is not null
     */
    public Console(KVDatabase kvdb, Database db, JSimpleDB jdb, InputStream input, OutputStream output,
      Terminal terminal, String encoding, String appName) throws IOException {
        Preconditions.checkArgument((kvdb != null ? 1 : 0) + (db != null ? 1 : 0) + (jdb != null ? 1 : 0) == 1,
          "exactly one of kvdb, db or jdb must be not null");
        Preconditions.checkArgument(input != null, "null input");
        Preconditions.checkArgument(output != null, "null output");
        this.console = new ConsoleReader(appName, input, output, terminal, encoding);
        this.console.setBellEnabled(true);
        this.console.setHistoryEnabled(true);
        this.console.setHandleUserInterrupt(true);
        final PrintWriter writer = new PrintWriter(console.getOutput(), true);
        this.session = jdb != null ? new CliSession(jdb, writer, this) :
          db != null ? new CliSession(db, writer, this) : new CliSession(kvdb, writer, this);
    }

    /**
     * Get the associated JLine {@link ConsoleReader}.
     *
     * @return associated console reader
     */
    public ConsoleReader getConsoleReader() {
        return this.console;
    }

    /**
     * Get the associated {@link CliSession}.
     *
     * @return associated CLI session
     */
    public CliSession getSession() {
        return this.session;
    }

    /**
     * Configure the command history file.
     *
     * @param historyFile file for storing command history
     */
    public void setHistoryFile(File historyFile) {
        Preconditions.checkState(this.history == null, "history file already configured");
        try {
            this.history = new FileHistory(historyFile);
        } catch (IOException e) {
            // ignore
        }
        this.console.setHistory(this.history);
    }

    /**
     * Run this instance. This method blocks until the connected user exits the console.
     *
     * @throws IOException if an I/O error occurs
     */
    public void run() throws IOException {

        // Input buffer
        final StringBuilder lineBuffer = new StringBuilder();

        // Set up tab completion
        console.addCompleter(new ConsoleCompleter(lineBuffer));

        // Get prompt
        final String prompt;
        switch (this.session.getMode()) {
        case KEY_VALUE:
            prompt = "KeyValue> ";
            break;
        case CORE_API:
            prompt = "CoreAPI> ";
            break;
        case JSIMPLEDB:
            prompt = "JSimpleDB> ";
            break;
        default:
            throw new RuntimeException("internal error");
        }

        // Main command loop
        try {

            this.console.println("Welcome to JSimpleDB. You are in " + this.session.getMode() + " mode. Type `help' for help.");
            this.console.println();
            while (!session.isDone()) {

                // Read command line
                String line;
                try {
                    line = this.console.readLine(lineBuffer.length() == 0 ?
                      prompt : String.format("%" + (prompt.length() - 3) + "s-> ", ""));
                } catch (UserInterruptException e) {
                    this.console.print("^C");
                    line = null;
                }
                if (line == null) {
                    this.console.println();
                    break;
                }

                // Detect backslash continuations
                boolean continuation = false;
                if (line.length() > 0 && line.charAt(line.length() - 1) == '\\') {
                    line = line.substring(0, line.length() - 1) + "\n";
                    continuation = true;
                }

                // Append line to buffer
                lineBuffer.append(line);

                // Handle backslash continuations
                if (continuation)
                    continue;
                final ParseContext ctx = new ParseContext(lineBuffer.toString());

                // Skip initial whitespace
                ctx.skipWhitespace();

                // Ignore blank input
                if (ctx.getInput().length() == 0)
                    continue;

                // Parse command(s)
                final ArrayList<CliSession.Action> actions = new ArrayList<>();
                final boolean[] needMoreInput = new boolean[1];
                final boolean ok = this.session.performCliSessionAction(new CliSession.Action() {
                    @Override
                    public void run(CliSession session) {
                        try {
                            actions.addAll(Console.this.commandListParser.parse(session, ctx, false));
                        } catch (ParseException e) {
                            if (ctx.getInput().length() == 0)
                                needMoreInput[0] = true;
                            else
                                throw e;
                        }
                    }
                });
                if (needMoreInput[0]) {
                    lineBuffer.append('\n');
                    continue;
                }
                lineBuffer.setLength(0);
                if (!ok)
                    continue;

                // Execute commands
                for (CliSession.Action action : actions) {
                    if (!this.session.performCliSessionAction(action))
                        break;
                }

                // Proceed
                this.console.flush();
            }
        } finally {
            if (this.history != null)
                this.history.flush();
            this.console.flush();
            this.console.shutdown();
        }
    }

    /**
     * Parse the given command(s).
     *
     * @param text command input
     * @return command actions, or null if there was an error during parsing
     * @throws ParseException if {@code text} cannot be parsed
     * @throws IllegalArgumentException if {@code text} is null
     */
    public List<CliSession.Action> parseCommand(String text) {
        Preconditions.checkArgument(text != null, "null text");
        final ParseContext ctx = new ParseContext(text);
        final ArrayList<CliSession.Action> actions = new ArrayList<>();
        return this.session.performCliSessionAction(new CliSession.Action() {
            @Override
            public void run(CliSession session) {
                actions.addAll(commandListParser.parse(session, ctx, false));
            }
        }) ? actions : null;
    }

// ConsoleCompleter

    private class ConsoleCompleter implements Completer {

        private final StringBuilder lineBuffer;

        ConsoleCompleter(StringBuilder lineBuffer) {
            this.lineBuffer = lineBuffer;
        }

        @Override
        public int complete(final String buffer, final int cursor, final List<CharSequence> candidates) {
            final ParseContext ctx = new ParseContext(this.lineBuffer + buffer.substring(0, cursor));
            try {
                Console.this.commandListParser.parse(Console.this.session, ctx, true);
            } catch (ParseException e) {
                String prefix = "";
                int index = ctx.getIndex();
                while (index > 0 && Character.isJavaIdentifierPart(ctx.getOriginalInput().charAt(index - 1)))
                    prefix = ctx.getOriginalInput().charAt(--index) + prefix;
                candidates.addAll(Lists.transform(e.getCompletions(), new AddPrefixFunction(prefix)));
                return index;
            } catch (Exception e) {
                try {
                    Console.this.console.println();
                    Console.this.console.println("Error: got exception calculating command line completions");
                    e.printStackTrace(Console.this.session.getWriter());
                } catch (IOException e2) {
                    // ignore
                }
            }
            return cursor;
        }
    }
}

