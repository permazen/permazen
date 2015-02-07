
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

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

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ConsoleReader console;
    private final CliSession session;
    private final CommandParser commandParser = new CommandParser();
    private final CommandListParser commandListParser = new CommandListParser(this.commandParser);

    private FileHistory history;

    /**
     * Constructor for core level access only.
     *
     * @param db core API {@link Database}
     * @param input console input
     * @param output console output
     */
    public Console(Database db, InputStream input, OutputStream output) throws IOException {
        this(db, null, input, output, null, null, null);
    }

    /**
     * Constructor for {@link JSimpleDB} level access.
     *
     * @param jdb {@link JSimpleDB} database
     * @param input console input
     * @param output console output
     */
    public Console(JSimpleDB jdb, InputStream input, OutputStream output) throws IOException {
        this(null, jdb, input, output, null, null, null);
    }

    /**
     * Primary constructor.
     *
     * @param db core API {@link Database}; must be null if and only if {@code jdb} is not null
     * @param jdb {@link JSimpleDB} database; must be null if and only if {@code db} is not null
     * @param input console input
     * @param output console output
     * @param terminal JLine terminal interface, or null for default
     * @param encoding character encoding for {@code terminal}, or null for default
     * @param appName JLine application name, or null for none
     * @throws IllegalArgumentException if {@code db} and {@code jdb} are both null or both not null
     */
    public Console(Database db, JSimpleDB jdb, InputStream input, OutputStream output,
      Terminal terminal, String encoding, String appName) throws IOException {
        if (!((jdb == null) ^ (db == null)))
            throw new IllegalArgumentException("exactly one of db or jdb must be null");
        if (input == null)
            throw new IllegalArgumentException("null input");
        if (output == null)
            throw new IllegalArgumentException("null output");
        this.console = new ConsoleReader(appName, input, output, terminal, encoding);
        this.console.setBellEnabled(true);
        this.console.setHistoryEnabled(true);
        this.console.setHandleUserInterrupt(true);
        final PrintWriter writer = new PrintWriter(console.getOutput(), true);
        this.session = jdb != null ? new CliSession(jdb, writer) : new CliSession(db, writer);
    }

    /**
     * Get the associated JLine {@link ConsoleReader}.
     */
    public ConsoleReader getConsoleReader() {
        return this.console;
    }

    /**
     * Get the associated {@link CliSession}.
     */
    public CliSession getSession() {
        return this.session;
    }

    /**
     * Configure the command history file.
     */
    public void setHistoryFile(File historyFile) {
        if (this.history != null)
            throw new IllegalStateException("history file already configured");
        try {
            this.history = new FileHistory(historyFile);
        } catch (IOException e) {
            // ignore
        }
        this.console.setHistory(this.history);
    }

    /**
     * Run this instance. This method blocks until the connected user exits the console.
     */
    public void run() throws IOException {

        // Input buffer
        final StringBuilder lineBuffer = new StringBuilder();

        // Set up tab completion
        console.addCompleter(new ConsoleCompleter(lineBuffer));

        // Main command loop
        try {

            this.console.println("Welcome to JSimpleDB. You are in "
              + (this.session.hasJSimpleDB() ? "JSimpleDB" : "Core API") + " CLI Mode. Type `help' for help.");
            this.console.println();
            while (!session.isDone()) {

                // Read command line
                String line;
                try {
                    line = this.console.readLine(lineBuffer.length() == 0 ? "JSimpleDB> " : "        -> ");
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
                final boolean ok = this.session.perform(new CliSession.Action() {
                    @Override
                    public void run(CliSession session) {
                        try {
                            actions.addAll(commandListParser.parse(session, ctx, false));
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
                    if (!this.session.perform(action))
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

// ConsoleCompleter

    private class ConsoleCompleter implements Completer {

        private final StringBuilder lineBuffer;

        ConsoleCompleter(StringBuilder lineBuffer) {
            this.lineBuffer = lineBuffer;
        }

        @Override
        public int complete(final String buffer, final int cursor, final List<CharSequence> candidates) {
            final int[] result = new int[1];
            Console.this.session.perform(new CliSession.Action() {
                @Override
                public void run(CliSession session) {
                    result[0] = ConsoleCompleter.this.completeInTransaction(session, buffer, cursor, candidates);
                }
            });
            return result[0];
        }

        private int completeInTransaction(CliSession session, String buffer, int cursor, List<CharSequence> candidates) {
            final ParseContext ctx = new ParseContext(this.lineBuffer + buffer.substring(0, cursor));
            try {
                Console.this.commandListParser.parse(session, ctx, true);
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
                    e.printStackTrace(session.getWriter());
                } catch (IOException e2) {
                    // ignore
                }
            }
            return cursor;
        }
    }
}

