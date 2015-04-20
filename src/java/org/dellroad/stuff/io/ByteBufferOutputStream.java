
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

/**
 * Provides an {@link OutputStream} view of a {@link ByteBuffer}.
 *
 * <p>
 * Instances are thread safe.
 * </p>
 */
public class ByteBufferOutputStream extends OutputStream {

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
    public ByteBufferOutputStream(ByteBuffer buf) {
        if (buf == null)
            throw new IllegalArgumentException("null buf");
        this.buf = buf;
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    @Override
    public synchronized void flush() throws IOException {
        if (this.closed)
            throw new IOException("stream is closed");
    }

    @Override
    public synchronized void write(int value) throws IOException {
        if (this.closed)
            throw new IOException("stream is closed");
        try {
            this.buf.put((byte)value);
        } catch (ReadOnlyBufferException | BufferOverflowException e) {
            throw new IOException("exception from underlying ByteBuffer", e);
        }
    }

    @Override
    public synchronized void write(byte[] data, int off, int len) throws IOException {
        if (this.closed)
            throw new IOException("stream is closed");
        try {
            this.buf.put(data, off, len);
        } catch (ReadOnlyBufferException | BufferOverflowException e) {
            throw new IOException("exception from underlying ByteBuffer", e);
        }
    }
}

