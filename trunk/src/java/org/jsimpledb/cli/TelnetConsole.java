
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.core.Database;

import jline.TerminalFactory;

/**
 * A {@link Console} connected to a telnet client via a {@link Socket}.
 *
 * <p>
 * This class can be used to implement an embedded CLI console within an application, accessed via telnet. For example:
 * <pre>
 *      // Accept a new connection
 *      final Socket socket = ...
 *
 *      // Set up telnet CLI console
 *      final TelnetConsole console = TelnetConsole.create(database, socket);
 *      console.getSession().registerStandardFunctions();
 *      console.getSession().registerStandardCommands();
 *
 *      // Run the console
 *      console.run();
 * </pre>
 * </p>
 */
public final class TelnetConsole extends Console {

    /**
     * Internal constructor.
     */
    private TelnetConsole(Database db, JSimpleDB jdb, InputStream input, OutputStream output,
      jline.Terminal terminal, String encoding, String appName) throws IOException {
        super(db, jdb, input, output, terminal, encoding, appName);
    }

    /**
     * Simplified factory method for core API CLI mode.
     *
     * @param db core API {@link Database}
     * @param socket socket connected to telnet client
     */
    public static TelnetConsole create(Database db, Socket socket) throws IOException {
        return TelnetConsole.create(db, null, socket.getInputStream(), socket.getOutputStream(), null, null);
    }

    /**
     * Simplified factory method for {@link JSimpleDB} API CLI mode.
     *
     * @param jdb {@link JSimpleDB} database
     * @param socket socket connected to telnet client
     */
    public static TelnetConsole create(JSimpleDB jdb, Socket socket) throws IOException {
        return TelnetConsole.create(null, jdb, socket.getInputStream(), socket.getOutputStream(), null, null);
    }

    /**
     * Generic factory method.
     *
     * @param db core API {@link Database}; must be null if and only if {@code jdb} is not null
     * @param jdb {@link JSimpleDB} database; must be null if and only if {@code db} is not null
     * @param input console input
     * @param output console output
     * @param encoding character encoding for {@code terminal}, or null for default
     * @param appName JLine application name, or null for none
     * @throws IllegalArgumentException if {@code db} and {@code jdb} are both null or both not null
     */
    public static TelnetConsole create(Database db, JSimpleDB jdb, InputStream input, OutputStream output,
      String encoding, String appName) throws IOException {

        // Set up nvt4j; ignore the initial clear & reposition
        final nvt4j.impl.Terminal nvt4jTerminal = new nvt4j.impl.Terminal(input, output) {

            private boolean cleared;
            private boolean moved;

            @Override
            public void clear() throws IOException {
                if (this.cleared)
                    super.clear();
                this.cleared = true;
            }

            @Override
            public void move(int row, int col) throws IOException {
                if (this.moved)
                    super.move(row, col);
                this.moved = true;
            }
        };
        nvt4jTerminal.put(nvt4j.impl.Terminal.AUTO_WRAP_ON);
        nvt4jTerminal.setCursor(true);

        // Have JLine do input & output through telnet terminal
        final InputStream jlineInput = new InputStream() {
            @Override
            public int read() throws IOException {
                return nvt4jTerminal.get();
            }
        };
        final OutputStream jlineOutput = new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                nvt4jTerminal.put(value);
            }
        };

        // Build console
        return new TelnetConsole(db, jdb, jlineInput, jlineOutput,
          new TelnetTerminal(TerminalFactory.get(), nvt4jTerminal), encoding, appName);
    }

// ForwardingTerminal

    /**
     * Wrapper class for a JLine {@link jline.Terminal} that forwards all methods to the wrapped delegate.
     */
    public static class ForwardingTerminal implements jline.Terminal {

        protected final jline.Terminal terminal;

        public ForwardingTerminal(jline.Terminal terminal) {
            if (terminal == null)
                throw new IllegalArgumentException("null terminal");
            this.terminal = terminal;
        }

        @Override
        public void init() throws Exception {
            this.terminal.init();
        }

        @Override
        public void restore() throws Exception {
            this.terminal.restore();
        }

        @Override
        public void reset() throws Exception {
            this.terminal.reset();
        }

        @Override
        public boolean isSupported() {
            return this.terminal.isSupported();
        }

        @Override
        public int getWidth() {
            return this.terminal.getWidth();
        }

        @Override
        public int getHeight() {
            return this.terminal.getHeight();
        }

        @Override
        public boolean isAnsiSupported() {
            return this.terminal.isAnsiSupported();
        }

        @Override
        public OutputStream wrapOutIfNeeded(OutputStream out) {
            return this.terminal.wrapOutIfNeeded(out);
        }

        @Override
        public InputStream wrapInIfNeeded(InputStream in) throws IOException {
            return this.terminal.wrapInIfNeeded(in);
        }

        @Override
        public boolean hasWeirdWrap() {
            return this.terminal.hasWeirdWrap();
        }

        @Override
        public boolean isEchoEnabled() {
            return this.terminal.isEchoEnabled();
        }

        @Override
        public void setEchoEnabled(boolean enabled) {
            this.terminal.setEchoEnabled(enabled);
        }

        @Override
        public String getOutputEncoding() {
            return this.terminal.getOutputEncoding();
        }
    }

// TelnetTerminal

    /**
     * JLine {@link jline.Terminal} that gets window size information from an associated nvt4j {@link nvt4j.impl.Terminal}.
     */
    public static class TelnetTerminal extends ForwardingTerminal {

        private final nvt4j.impl.Terminal nvt;

        public TelnetTerminal(jline.Terminal terminal, nvt4j.impl.Terminal nvt) {
            super(terminal);
            if (nvt == null)
                throw new IllegalArgumentException("null nvt");
            this.nvt = nvt;
        }

        @Override
        public int getWidth() {
            final int width = this.nvt.getColumns();
            return width > 0 ? width : super.getWidth();
        }

        @Override
        public int getHeight() {
            final int height = this.nvt.getRows();
            return height > 0 ? height : super.getHeight();
        }

        // TODO: echoEnabled <-> EchoOptionHandler, possibly others
    }
}

