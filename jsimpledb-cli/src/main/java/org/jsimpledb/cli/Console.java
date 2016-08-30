
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
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.dellroad.stuff.java.ProcessRunner;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.cli.cmd.EvalCommand;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.util.AddPrefixFunction;
import org.jsimpledb.util.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.Terminal;
import jline.TerminalFactory;
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
        if (terminal == null)
            terminal = Console.getTerminal();
        this.console = new ConsoleReader(appName, input, output, terminal, encoding);
        this.console.setBellEnabled(true);
        this.console.setHistoryEnabled(true);
        this.console.setHandleUserInterrupt(true);
        this.console.setExpandEvents(false);
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
     * Set/update the command history file. May be reconfigured while executing.
     *
     * @param historyFile file for storing command history
     * @throws IOException if {@code historyFile} cannot be read
     * @throws IllegalArgumentException if {@code historyFile} is null
     */
    public void setHistoryFile(File historyFile) throws IOException {

        // Sanity check
        Preconditions.checkArgument(historyFile != null, "null historyFile");

        // Open new history
        final FileHistory newHistory = new FileHistory(historyFile);

        // Close current history
        if (this.history != null)
            this.history.flush();

        // Replace in console
        this.history = newHistory;
        this.console.setHistory(this.history);
    }

    /**
     * Run this instance in non-interactive (or "batch") mode on the given input.
     *
     * @param input command input; is not closed by this method
     * @param inputDescription description of input (e.g., file name) used for error reporting, or null for none
     * @return true if successful, false if an error occurred
     * @throws IOException if an I/O error occurs
     */
    public boolean runNonInteractive(Reader input, String inputDescription) throws IOException {

        // Read entire input in as a string XXX currently we don't have the capability to parse streams
        final StringWriter data = new StringWriter();
        final char[] buf = new char[1024];
        int r;
        while ((r = input.read(buf)) != -1)
            data.write(buf, 0, r);
        String text = data.toString();

        // Remove comments
        text = text.replaceAll("(?m)^[\\s&&[^\\n]]*#.*$", "");

        // Parse and execute commands one at a time
        final ParseContext ctx = new ParseContext(text);
        final CliSession.Action[] action = new CliSession.Action[1];
        boolean error = false;
        while (true) {

            // Skip whitespace
            ctx.skipWhitespace();
            if (ctx.isEOF())
                break;

            // Set new error prefix while handling the next command
            final String previousErrorMessagePrefix = this.session.getErrorMessagePrefix();
            final int lineNumber = text.substring(0, ctx.getIndex()).replaceAll("[^\\n]", "").length() + 1;
            this.session.setErrorMessagePrefix(previousErrorMessagePrefix
              + (inputDescription != null ? inputDescription + ": " : "") + "line " + lineNumber + ": ");
            try {

                // Parse next command
                if (!this.session.performCliSessionAction(new CliSession.Action() {
                    @Override
                    public void run(CliSession session) {
                        action[0] = Console.this.commandParser.parse(session, ctx, false);
                    }
                })) {
                    error = true;
                    break;
                }

                // Execute command
                if (!this.session.performCliSessionAction(action[0])
                  || (action[0] instanceof EvalCommand.EvalAction
                   && ((EvalCommand.EvalAction)action[0]).getEvalException() != null)) {
                    error = true;
                    break;
                }
            } finally {
                this.session.setErrorMessagePrefix(previousErrorMessagePrefix);
            }

            // Skip whitespace
            ctx.skipWhitespace();
            if (ctx.isEOF())
                break;

            // Expecte semi-colon separator
            if (!ctx.tryLiteral(";")) {
                this.session.reportException(new ParseException(ctx, "expected `;'"));
                error = true;
                break;
            }
        }

        // Flush output
        this.console.flush();

        // Done
        return !error;
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
        this.console.addCompleter(new ConsoleCompleter(lineBuffer));

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

    /**
     * Get the {@link Terminal} instance appropriate for this operating system.
     *
     * @return JLine {@link Terminal} to use
     */
    public static Terminal getTerminal() throws IOException {

        // Are we running on Windows under Cygwin? If so use UNIX flavor instead of Windows
        final boolean windows = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).indexOf("win") != -1;
        while (windows) {
            final ProcessRunner runner;
            try {
                runner = new ProcessRunner(Runtime.getRuntime().exec(new String[] { "uname", "-s" }));
            } catch (IOException e) {
                break;
            }
            runner.setDiscardStandardError(true);
            try {
                runner.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (!new String(runner.getStandardOutput(), StandardCharsets.UTF_8).trim().matches("(?is)^cygwin.*"))
                break;
            try {
                return TerminalFactory.getFlavor(TerminalFactory.Flavor.UNIX);
            } catch (Exception e) {
                break;
            }
        }

        // Do the normal thing
        return TerminalFactory.get();
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

