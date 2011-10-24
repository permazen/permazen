
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads zero or more {@link InputStream}s serialized inside an underlying {@link InputStream} by an {@link OutputStreamWriter}.
 *
 * <p>
 * Instances of this class are thread safe, as are the {@link InputStream}s returned by {@link #read}.
 * </p>
 *
 * @see OutputStreamWriter
 */
public class InputStreamReader {

    // Escape codes
    static final int CONTROL_ESCAPE = 1;
    static final int CONTROL_SEPARATOR = 2;

    // Special return values
    private static final int RESULT_EOF = -1;               // read EOF on input
    private static final int RESULT_SEPARATOR = -2;         // read stream separator

    private final RandomEscape randomEscape = new RandomEscape();
    private final InputStream input;

    private NestedInputStream current;          // curently active nested input
    private IOException exception;              // exception thrown on input
    private int escape;                         // current escape character
    private boolean closed;                     // whether this instance is closed
    private boolean eof;                        // whether this instance has read EOF

    /**
     * Constructor.
     *
     * @param input the underlying {@link InputStream} that will carry nested {@link InputStream}s within it
     */
    public InputStreamReader(InputStream input) {
        this.input = input;
        this.escape = this.randomEscape.next();
    }

    /**
     * Read the next {@link InputStream}.
     *
     * <p>
     * The returned {@link InputStream} will remain valid as long as it has not been closed, the underlying input
     * has not been closed, and no subsequent invocation of this method has been made. As soon as any of those events occurs,
     * all subsequent accesses to the previously returned {@link InputStream} will throw an exception.
     * </p>
     *
     * <p>
     * The returned {@link InputStream} may be closed prior to its EOF, in which case any remaining bytes will be skipped
     * over during the next invocation of this method.
     * </p>
     *
     * <p>
     * Note: while any thread is blocked reading from the returned {@link InputStream}, this method will block as well.
     * </p>
     *
     * @throws IOException if this instance is closed
     * @throws IOException if the underlying {@link InputStream} has thrown an exception
     * @throws return the next {@link InputStream}, or {@code null} if EOF has been reached on the underlying input
     */
    public synchronized InputStream read() throws IOException {

        // Check state
        if (this.closed)
            throw new IOException("this instance is closed");
        if (this.exception != null)
            throw new IOException("exception on the underlying stream", this.exception);
        if (this.eof)
            return null;

        // Close current stream (if any) and skip past abandoned bytes (if any)
        if (this.current != null) {
            this.current.close();
            if (!this.current.isEOF()) {
                while (true) {
                    if (this.readNext() < 0)
                        break;
                }
            }
        }

        // See what's next up
        int firstValue = this.readNext();
        if (firstValue == RESULT_EOF)
            return null;

        // Create new nested stream
        this.current = new NestedInputStream(firstValue);
        return this.current;
    }

    // Read next byte (unescaped) or special return value
    private int readNext() throws IOException {

        // Already read EOF?
        if (this.eof)
            return RESULT_EOF;

        // Read next byte
        int ch = this.input.read();
        if (ch == -1) {
            this.eof = true;
            return RESULT_EOF;
        }

        // Check for escape byte
        if (ch == this.escape) {

            // Read escaped byte
            if ((ch = this.input.read()) == -1) {
                this.eof = true;
                return RESULT_EOF;
            }
            ch ^= this.escape;

            // Advance escape character for next time
            int prevEscape = this.escape;
            this.escape = this.randomEscape.next();

            // Check control code
            switch (ch) {
            case CONTROL_ESCAPE:
                return prevEscape;
            case CONTROL_SEPARATOR:
                return RESULT_SEPARATOR;
            default:
                break;
            }
            throw new IOException("rec'd unexpected escape code " + ch);
        }

        // Just a normal character
        return ch;
    }

    /**
     * Close this instance. Does nothing if already closed.
     *
     * <p>
     * This closes the underlying input; however, if the {@link InputStream} most recently returned from {@link #read}
     * is still open, the close of the underlying input will be postponed until it is no longer open.
     * </p>
     *
     * @throws IOException if an there is an error closing the underlying {@link InputStream}
     */
    public synchronized void close() throws IOException {
        if (this.closed)
            return;
        this.closed = true;
        if (this.current != null)
            this.current.checkInputClose();
    }

    private class NestedInputStream extends FilterInputStream {

        private int firstValue;
        private boolean firstRead = true;
        private boolean closed;
        private boolean eof;

        public NestedInputStream(int firstValue) {
            super(InputStreamReader.this.input);
            this.firstValue = firstValue;
        }

        @Override
        public int read() throws IOException {
            synchronized (InputStreamReader.this) {

                // Check state
                if (this.closed)
                    throw new IOException("stream is closed");
                if (InputStreamReader.this.exception != null)
                    throw new IOException("exception on the underlying stream", InputStreamReader.this.exception);
                if (this.eof)
                    return -1;

                // Read from the underlying stream
                try {

                    // Read next unescaped byte or control code
                    int ch;
                    if (this.firstRead) {
                        ch = this.firstValue;
                        this.firstRead = false;
                    } else
                        ch = InputStreamReader.this.readNext();
                    switch (ch) {
                    case RESULT_EOF:
                        throw new IOException("underlying stream was truncated");
                    case RESULT_SEPARATOR:
                        this.eof = true;
                        return -1;
                    default:
                        break;
                    }

                    // Done
                    return ch;
                } catch (IOException e) {
                    InputStreamReader.this.exception = e;
                    throw e;
                }
            }
        }

        /**
         * Close this instance. Does nothing if already closed.
         */
        @Override
        public void close() throws IOException {
            synchronized (InputStreamReader.this) {
                if (this.closed)
                    return;
                this.closed = true;
                this.checkInputClose();
            }
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (len < 0 || off < 0 || off + len > buf.length)
                throw new IndexOutOfBoundsException();
            int count = 0;
            while (count < len) {
                int ch = this.read();
                if (ch == -1) {
                    if (count == 0)
                        return -1;
                    break;
                }
                buf[off++] = (byte)ch;
                count++;
            }
            return count;
        }

        @Override
        public long skip(long num) throws IOException {
            long count = 0;
            while (count < num && this.read() != -1)
                count++;
            return count;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void mark(int readlimit) {
        }

        @Override
        public void reset() throws IOException {
            throw new IOException("mark/reset not supported");
        }

        public void checkInputClose() throws IOException {
            synchronized (InputStreamReader.this) {
                if (this.closed && InputStreamReader.this.closed)
                    this.in.close();
            }
        }

        public boolean isEOF() {
            return this.eof;
        }
    }
}

