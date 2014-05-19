
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
import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.core.Database;
import org.jsimpledb.util.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.Completer;
import jline.console.history.FileHistory;

/**
 * CLI console.
 */
public class Console {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Database db;
    private final ConsoleReader console;
    private final Session session;

    private FileHistory history;

    /**
     * Constrcutor.
     */
    public Console(Database db, InputStream in, OutputStream out) throws IOException {
        this.db = db;
        this.console = new ConsoleReader(in, out);
        this.console.setBellEnabled(true);
        this.console.setHistoryEnabled(true);
        this.console.setHandleUserInterrupt(true);
        this.session = new Session(this.db, this.console);
    }

    public Database getDatabase() {
        return this.db;
    }

    public ConsoleReader getConsole() {
        return this.console;
    }

    public Session getSession() {
        return this.session;
    }

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
     * Run this instance.
     */
    public void run() throws IOException {

        // Input buffer
        final StringBuilder lineBuffer = new StringBuilder();

        // Set up tab completion
        console.addCompleter(new ConsoleCompleter(lineBuffer));

        // Open a transaction to load and verify database schema
        this.session.perform(new Action() {
            @Override
            public void run(Session session) { }
        });

        // Main command loop
        try {

            this.console.println("Welcome to JSimpleDB. Type `help' for help.");
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
                lineBuffer.setLength(0);

                // Skip initial whitespace
                ctx.skipWhitespace();

                // Ignore blank input
                if (ctx.getInput().length() == 0)
                    continue;

                // Parse command(s)
                final ArrayList<Action> actions = new ArrayList<>();
                if (!this.session.perform(new Action() {
                    @Override
                    public void run(Session session) {
                        actions.addAll(new CommandListParser().parse(session, ctx, false));
                    }
                }))
                    continue;

                // Execute commands
                for (Action action : actions) {
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
            Console.this.session.perform(new Action() {
                @Override
                public void run(Session session) {
                    result[0] = ConsoleCompleter.this.completeInTransaction(session, buffer, cursor, candidates);
                }
            });
            return result[0];
        }

        private int completeInTransaction(Session session, String buffer, int cursor, List<CharSequence> candidates) {
            final ParseContext ctx = new ParseContext(this.lineBuffer + buffer.substring(0, cursor));
            try {
                new CommandListParser().parse(session, ctx, true);
            } catch (ParseException e) {
                String prefix = "";
                int index = ctx.getIndex();
                while (index > 0 && !Character.isWhitespace(ctx.getOriginalInput().charAt(index - 1)))
                    prefix = ctx.getOriginalInput().charAt(--index) + prefix;
                candidates.addAll(Lists.transform(e.getCompletions(), new AddPrefixFunction(prefix)));
                return index;
            } catch (Exception e) {
                try {
                    Console.this.console.println();
                    Console.this.console.println("ERROR: error calculating command line completions");
                    e.printStackTrace(session.getWriter());
                } catch (IOException e2) {
                    // ignore
                }
            }
            return cursor;
        }
    }
}

