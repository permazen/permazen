
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

/**
 * Provides an {@link InputStream} view of a {@link ByteBuffer}.
 *
 * <p>
 * Instances are thread safe.
 * </p>
 */
public class ByteBufferInputStream extends InputStream {

    /**
     * The underlying {@link ByteBuffer}.
     */
    protected final ByteBuffer buf;

    private boolean closed;

    /**
     * Constructor.
     *
     * @param buf buffer
     * @throws IllegalArgumentException if {@code buf} is null
     */
    public ByteBufferInputStream(ByteBuffer buf) {
        if (buf == null)
            throw new IllegalArgumentException("null buf");
        this.buf = buf;
    }

    @Override
    public synchronized int read() throws IOException {
        if (this.closed)
            throw new IOException("stream is closed");
        if (!this.buf.hasRemaining())
            return -1;
        return this.buf.get() & 0xff;
    }

    @Override
    public synchronized int read(byte[] data, int off, int len) throws IOException {
        if (this.closed)
            throw new IOException("stream is closed");
        if (!this.buf.hasRemaining())
            return -1;
        len = Math.min(len, this.buf.remaining());
        this.buf.get(data, off, len);
        return len;
    }

    @Override
    public synchronized long skip(long amount) throws IOException {
        if (this.closed)
            throw new IOException("stream is closed");
        final int skip = (int)Math.min(amount, this.buf.remaining());
        if (skip <= 0)
            return 0;
        this.buf.position(this.buf.position() + skip);
        return skip;
    }

    @Override
    public synchronized int available() throws IOException {
        if (this.closed)
            throw new IOException("stream is closed");
        return this.buf.remaining();
    }

    @Override
    public synchronized void mark(int readlimit) {
        if (this.closed)
            return;
        this.buf.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        if (this.closed)
            throw new IOException("stream is closed");
        try {
            this.buf.reset();
        } catch (InvalidMarkException e) {
            throw new IOException("no mark set", e);
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }
}

