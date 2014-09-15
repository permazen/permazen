
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.dellroad.stuff.java.Predicate;
import org.dellroad.stuff.java.TimedWait;

/**
 * Wraps an {@link InputStream} and enforces a maximum time limit on how long any {@link InputStream#read read()}
 * operation may block. If the time limit is exceeded, an {@link IdleTimeoutException} is thrown.
 *
 * <p>
 * As a side effect of its design, this class may also be used to artificially inject data, EOF, or exceptions into the
 * {@code InputStream}, using the {@link AsyncInputStream.Listener} interface methods.
 * </p>
 *
 * <p>
 * This class may also be used to allow testing the state of an {@link InputStream} without actually reading
 * from it, via {@link #available}, which guarantees an immediate exception will be thrown if the underlying
 * {@link InputStream} has been closed, reached EOF, or thrown an exception (this is a stronger guarantee than
 * required by the {@link InputStream} interface).
 * </p>
 *
 * <p>
 * All methods in this class are thread safe.
 * </p>
 */
public class IdleTimeoutInputStream extends InputStream implements AsyncInputStream.Listener {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static final int OPEN = 0;
    private static final int EOF = 1;
    private static final int EXCEPTION = 2;
    private static final int CLOSED = 3;

    private final AsyncInputStream asyncInputStream;
    private final long timeout;

    private final byte[] xferBuf = new byte[500];
    private Throwable exception;
    private int xferLen;
    private int state;

    /**
     * Constructor.
     *
     * @param in input source
     * @param threadName name for the reader thread, or null for default
     * @param timeout maximum input idle time, or zero for infinity
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    public IdleTimeoutInputStream(InputStream in, String threadName, long timeout) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout < 0");
        if (threadName == null)
            threadName = this.getClass().getSimpleName() + "-" + IdleTimeoutInputStream.COUNTER.incrementAndGet();
        this.asyncInputStream = new AsyncInputStream(in, threadName, this);
        this.timeout = timeout;
    }

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote><code>
     *  IdleTimeoutInputStream(in, null, timeout);
     *  </code></blockquote>
     *
     * @param in input source
     * @param timeout maximum input idle time
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    public IdleTimeoutInputStream(InputStream in, long timeout) {
        this(in, null, timeout);
    }

// InputStream

    @Override
    public synchronized int read() throws IOException {

        // Wait for some data
        if (!this.waitForData(this.timeout))
            return -1;

        // Wake up sleeping writer, if any
        if (this.xferLen == this.xferBuf.length)
            this.notifyAll();

        // Read off byte
        final int r = this.xferBuf[0] & 0xff;
        System.arraycopy(this.xferBuf, 1, this.xferBuf, 0, --this.xferLen);
        return r;
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {

        // Sanity check
        if (off < 0)
            throw new IndexOutOfBoundsException("off < 0");
        if (len < 0)
            throw new IndexOutOfBoundsException("len < 0");
        if (off + len > buf.length)
            throw new IndexOutOfBoundsException("off + len > buf.length");

        // Wait for some data
        if (!this.waitForData(this.timeout))
            return -1;

        // Wake up sleeping writer, if any
        if (this.xferLen == this.xferBuf.length)
            this.notifyAll();

        // Read off bytes
        len = Math.min(len, this.xferLen);
        System.arraycopy(this.xferBuf, 0, buf, 0, len);
        System.arraycopy(this.xferBuf, len, this.xferBuf, 0, (this.xferLen -= len));
        return len;
    }

    // Wait up to 'duration' milliseconds for something to happen; or if negative, just check status
    private synchronized boolean waitForData(final long duration) throws IOException {

        // Wait for data or exception
        for (boolean timedOut = false; !timedOut; ) {

            // Check state
            switch (this.state) {
            case OPEN:
                if (this.xferLen > 0)
                    return true;
                break;
            case EOF:
                return false;
            case EXCEPTION:
                if (this.exception instanceof IOException)
                    throw (IOException)this.exception;
                if (this.exception instanceof RuntimeException)
                    throw (RuntimeException)this.exception;
                throw new RuntimeException(this.exception);                 // should never happen
            case CLOSED:
                throw new IOException("stream is closed");
            default:
                throw new RuntimeException("internal error");
            }

            // Not waiting, just checking status?
            if (duration < 0)
                return false;

            // Wait for some data to appear, timeout, EOF, exception, or closure
            try {
                timedOut = !TimedWait.wait(this, duration, new Predicate() {
                    @Override
                    public boolean test() {
                        return IdleTimeoutInputStream.this.state != OPEN || IdleTimeoutInputStream.this.xferLen > 0;
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();                         // start over waiting!
            }
        }

        // We timed out; throw IdleTimeoutException
        this.asyncInputStream.close();
        this.exception = new IdleTimeoutException(duration);
        this.state = EXCEPTION;
        throw (IOException)this.exception;
    }

    @Override
    public synchronized void close() {
        switch (this.state) {
        case OPEN:
        case EOF:
        case EXCEPTION:
            this.asyncInputStream.close();
            this.state = CLOSED;
            this.notifyAll();                   // read() and close() could be called by two different threads
            break;
        case CLOSED:
            break;
        default:
            throw new RuntimeException("internal error");
        }
    }

    /**
     * Check how may bytes may be read from this instance without blocking.
     *
     * <p>
     * The {@link IdleTimeoutInputStream} guarantees that if the underlying {@link InputStream} has been closed,
     * reached EOF, or thrown an exception, then this method will throw an immediate exception.
     * </p>
     *
     * @return the number of bytes may be read immediately without blocking
     * @throws IOException if the underlying {@link InputStream} threw an {@link IOException}
     */
    @Override
    public synchronized int available() throws IOException {
        return this.waitForData(-1) ? this.xferLen : 0;
    }

// AsyncInputStream.Listener

    @Override
    public synchronized void handleInput(byte[] buf, int off, int len) {
        while (len > 0) {

            // Check state
            switch (this.state) {
            case OPEN:
                break;
            case EOF:
            case EXCEPTION:
            case CLOSED:
                return;
            default:
                throw new RuntimeException("internal error");
            }

            // Copy what we can, if anything
            final int copy = Math.min(len, this.xferBuf.length - this.xferLen);
            if (copy > 0) {

                // Copy data
                System.arraycopy(buf, off, this.xferBuf, this.xferLen, copy);
                this.xferLen += copy;
                off += copy;
                len -= copy;

                // Notify sleeping readers, if any
                if (this.xferLen == copy)
                    this.notifyAll();
                continue;
            }

            // Wait until there's more room in the transfer buffer
            try {
                TimedWait.wait(this, 0, new Predicate() {
                    @Override
                    public boolean test() {
                        return IdleTimeoutInputStream.this.state != OPEN
                          || IdleTimeoutInputStream.this.xferLen < IdleTimeoutInputStream.this.xferBuf.length;
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public synchronized void handleEOF() {
        switch (this.state) {
        case OPEN:
            this.state = EOF;
            this.notifyAll();
            break;
        case EOF:
        case EXCEPTION:
        case CLOSED:
            return;
        default:
            throw new RuntimeException("internal error");
        }
    }

    @Override
    public synchronized void handleException(Throwable e) {
        switch (this.state) {
        case OPEN:
            this.state = EXCEPTION;
            this.exception = e;
            this.notifyAll();
            break;
        case EOF:
        case EXCEPTION:
        case CLOSED:
            return;
        default:
            throw new RuntimeException("internal error");
        }
    }
}

