
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Serializes zero or more {@link OutputStream}s inside a single underlying {@link OutputStream}. The results can be
 * deserialized as equally many distinct {@link java.io.InputStream}s on the other end using an {@link InputStreamReader}.
 * Each {@link OutputStream} may contain an arbitrary amount of data.
 *
 * <p>
 * To use this class, invoke {@link #start} to start a new {@link OutputStream}, write to it by writing to this
 * class normally, and then use {@link #stop} to close the current {@link OutputStream}. A new, subsequent {@link OutputStream}
 * is created by invoking {@link #start} again.
 * </p>
 *
 * <p>
 * Each {@link OutputStream} written in this way will be read as distinct {@link java.io.InputStream} by the
 * {@link InputStreamReader} at the other end.
 * </p>
 *
 * <p>
 * Instances of this class are thread safe.
 * </p>
 *
 * @see InputStreamReader
 */
public class OutputStreamWriter extends FilterOutputStream {

    private final RandomEscape randomEscape = new RandomEscape();

    private int escape;
    private boolean started;
    private boolean closed;

    /**
     * Constructor.
     *
     * @param output the underlying {@link OutputStream} that will carry nested {@link OutputStream}s within it
     */
    public OutputStreamWriter(OutputStream output) {
        super(output);
        this.escape = this.randomEscape.next();
    }

    /**
     * Start a new {@link OutputStream}.
     *
     * @throws IOException if this instance is closed
     * @throws IOException if an {@link OutputStream} is already started
     */
    public synchronized void start() throws IOException {
        if (this.closed)
            throw new IOException("this instance is closed");
        if (this.started)
            throw new IOException("already started");
        this.started = true;
    }

    /**
     * End the current {@link OutputStream}. This flushes the underlying output.
     * A new {@link OutputStream} will be created upon the next invocation of {@link #start}.
     *
     * @throws IOException if this instance is closed
     * @throws IOException if no {@link OutputStream} is currently started
     * @throws IOException if the underlying {@link OutputStream} throws an exception
     */
    public synchronized void stop() throws IOException {
        if (this.closed)
            throw new IOException("this instance is closed");
        if (!this.started)
            throw new IOException("not started");
        this.started = false;
        this.writeControl(InputStreamReader.CONTROL_SEPARATOR);
        this.flush();
    }

    /**
     * Close this instance. Does nothing if already closed.
     * If there an {@link OutputStream} is already started when this method is invoked, it will be implicitly
     * {@linkplain #stop stopped}.
     *
     * <p>
     * This ends the current {@link OutputStream} and closes the underlying input.
     * </p>
     *
     * @throws IOException if an there is an error closing the underlying {@link OutputStream}
     */
    @Override
    public synchronized void close() throws IOException {
        if (this.closed)
            return;
        if (this.started) {
            this.started = false;
            this.writeControl(InputStreamReader.CONTROL_SEPARATOR);
        }
        this.closed = true;
        this.out.close();
    }

    @Override
    public synchronized void write(int ch) throws IOException {
        if (this.closed)
            throw new IOException("this instance is closed");
        if (!this.started)
            this.started = true;
        if ((ch & 0xff) == this.escape) {
            this.writeControl(InputStreamReader.CONTROL_ESCAPE);
            return;
        }
        this.out.write(ch);
    }

    private synchronized void writeControl(int control) throws IOException {
        this.out.write(this.escape);
        this.out.write(this.escape ^ control);
        this.escape = this.randomEscape.next();
    }
}

