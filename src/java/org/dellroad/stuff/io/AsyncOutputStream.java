
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.dellroad.stuff.java.CheckedExceptionWrapper;
import org.dellroad.stuff.java.Predicate;
import org.dellroad.stuff.java.TimedWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputStream} that performs writes using a background thread, so that
 * write, flush, and close operations never block.
 * <p/>
 * <p>
 * If the underlying output stream throws an {@link IOException} during any operation,
 * this instance will re-throw the exception for all subsequent operations.
 * </p>
 * <p/>
 * <p>
 * Instances use an internal buffer whose size is configured at construction time;
 * if the buffer overflows, a {@link BufferOverflowException} is thrown.
 * </p>
 * <p/>
 * <p>
 * Instances of this class are thread safe, and moreover writes are atomic: if multiple threads are writing
 * at the same time the bytes written in any single method invocation are written contiguously to the
 * underlying output.
 * </p>
 */
public class AsyncOutputStream extends FilterOutputStream {

    // Async thread linger time, to avoid rapid stop/start cycles
    private static final long THREAD_LINGER_TIME = 10000;               // 10 sec.

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final String name;
    private final byte[] buf;           // output buffer
    private int count;                  // number of bytes in output buffer ready to be written
    private int flushMark = -1;         // buffer byte at which a flush is requested, or -1 if none
    private Thread thread;              // async writer thread
    private IOException exception;      // exception caught by async thread
    private boolean closed;             // this instance has been close()'d

    /**
     * Constructor.
     *
     * @param out     underlying output stream
     * @param bufsize maximum number of bytes we can buffer
     * @param name    name for this instance; used to create the name of the background thread
     */
    public AsyncOutputStream(OutputStream out, int bufsize, String name) {
        super(out);
        if (out == null)
            throw new IllegalArgumentException("null output");
        this.name = name;
        this.buf = new byte[bufsize];
    }

    /**
     * Write data.
     * <p>
     * This method will never block. To effect a normal blocking write, use {@link #waitForSpace} first.
     * </p>
     *
     * @param b byte to write (lower 8 bits)
     * @throws IOException             if an exception has been thrown by the underlying stream
     * @throws IOException             if this instance has been closed
     * @throws BufferOverflowException if the buffer does not have room for the new byte
     */
    @Override
    public void write(int b) throws IOException {
        this.write(new byte[]{(byte)b}, 0, 1);
    }

    /**
     * Write data.
     * <p>
     * This method will never block. To effect a normal blocking write, invoke {@link #waitForSpace} first.
     * </p>
     *
     * @param data bytes to write
     * @param off  starting offset in buffer
     * @param len  number of bytes to write
     * @throws IOException              if an exception has been thrown by the underlying stream
     * @throws IOException              if this instance has been closed
     * @throws BufferOverflowException  if the buffer does not have room for the new data
     * @throws IllegalArgumentException if {@code len} is negative
     */
    @Override
    public synchronized void write(byte[] data, int off, int len) throws IOException {

        // Check exception conditions
        checkExceptions();
        if (this.count + len > this.buf.length)
            throw new BufferOverflowException(len + " more byte(s) would exceed the " + this.buf.length + " byte buffer");
        if (len < 0)
            throw new IllegalArgumentException("len = " + len);
        if (len == 0)
            return;

        // Add data to buffer
        System.arraycopy(data, off, this.buf, this.count, len);
        this.count += len;

        // Create/wakeup async thread
        wakeupAsyncThread();
    }

    /**
     * Flush output. This method will cause the underlying stream to be flushed once all of the data written to this
     * instance at the time this method is invoked has been written to it.
     * <p/>
     * <p>
     * This method will never block. To block until the underlying flush operation completes, invoke {@link #waitForIdle}.
     * </p>
     *
     * @throws IOException if this instance has been closed
     * @throws IOException if an exception has been detected on the underlying stream
     * @throws IOException if the current thread is interrupted; the nested exception will an {@link InterruptedException}
     */
    @Override
    public synchronized void flush() throws IOException {

        // Check exception conditions
        checkExceptions();

        // Indicate flush request
        this.flushMark = this.count;

        // Create/wakeup async thread
        wakeupAsyncThread();
    }

    /**
     * Close this instance. This method invokes {@link #flush} and then closes this instance
     * as well as the underlying output stream.
     * <p/>
     * <p>
     * If this instance has already been closed, nothing happens.
     * </p>
     * <p/>
     * <p>
     * This method will never block. To block until the underlying close operation completes, invoke {@link #waitForIdle}.
     * </p>
     *
     * @throws IOException if an exception has been detected on the underlying stream
     */
    @Override
    public synchronized void close() throws IOException {
        if (this.closed)
            return;
        flush();
        this.closed = true;
    }

    /**
     * Get the exception thrown by the underlying output stream, if any.
     *
     * @return thrown exception, or {@code null} if none has been thrown by the underlying stream
     */
    public synchronized IOException getException() {
        return this.exception;
    }

    /**
     * Get the capacity of this instance's output buffer.
     *
     * @return output buffer capacity configured at construction time
     */
    public synchronized int getBufferSize() {
        return this.buf.length;
    }

    /**
     * Get the number of free bytes remaining in the output buffer.
     *
     * @return current number of available bytes in the output buffer
     * @see #waitForSpace
     */
    public synchronized int availableBufferSpace() {
        return this.buf.length - this.count;
    }

    /**
     * Determine if there is outstanding work still to be performed (writes, flushes, and/or close operations)
     * by the background thread.
     *
     * @see #waitForIdle
     */
    public synchronized boolean isWorkOutstanding() {
        return this.count > 0 || this.flushMark != -1;
    }

    /**
     * Wait for buffer space availability.
     *
     * @param numBytes amount of buffer space required
     * @param timeout  maximum time to wait in milliseconds, or zero for infinite
     * @return true if space was found, false if time expired
     * @throws IOException              if this instance is or has been closed
     * @throws IOException              if an exception has been detected on the underlying stream
     * @throws IllegalArgumentException if {@code numBytes} is greater than the configured buffer size
     * @throws IllegalArgumentException if {@code timeout} is negative
     * @throws InterruptedException     if the current thread is interrupted
     * @see #availableBufferSpace
     */
    public boolean waitForSpace(final int numBytes, long timeout) throws IOException, InterruptedException {
        if (numBytes > this.buf.length)
            throw new IllegalArgumentException("numBytes (" + numBytes + ") > buffer size (" + this.buf.length + ")");
        return waitForPredicate(timeout, new Predicate() {
            @Override
            public boolean test() {
                return AsyncOutputStream.this.availableBufferSpace() >= numBytes;
            }
        });
    }

    /**
     * Wait for all outstanding work to complete.
     *
     * @param timeout maximum time to wait in milliseconds, or zero for infinite
     * @return true for success, false if time expired
     * @throws IOException              if this instance is or has been closed
     * @throws IOException              if an exception has been detected on the underlying stream
     * @throws IllegalArgumentException if {@code timeout} is negative
     * @throws InterruptedException     if the current thread is interrupted
     * @see #isWorkOutstanding
     */
    public synchronized boolean waitForIdle(long timeout) throws IOException, InterruptedException {
        return waitForPredicate(timeout, new Predicate() {
            @Override
            public boolean test() {
                return !AsyncOutputStream.this.isWorkOutstanding();
            }
        });
    }

    /**
     * (Re)start or wakeup the async thread as necessary.
     */
    private synchronized void wakeupAsyncThread() {

        // Is thread already running? If so, notify in case it's lingering
        if (this.thread != null) {
            this.notifyAll();                   // wake up thread sleeping in runLoop()
            return;
        }

        // Is there anything for the thread to do?
        if (!isWorkOutstanding())
            return;

        // Start a new thread
        this.thread = new Thread(this.name) {
            @Override
            public void run() {
                AsyncOutputStream.this.threadMain();
            }
        };
        this.thread.start();
    }

    /**
     * Check for exceptions.
     *
     * @throws IOException if this instance has been closed
     * @throws IOException if an exception has been detected on the underlying stream
     */
    private void checkExceptions() throws IOException {
        if (this.closed)
            throw new IOException("instance has been closed");
        if (this.exception != null)
            throw new IOException("exception from underlying output stream", this.exception);
    }

    /**
     * Writer thread main entry point.
     */
    private void threadMain() {
        try {
            this.runLoop();
        } catch (IOException e) {
            this.log.error(this.name + " caught exception", e);
            synchronized (this) {
                this.exception = e;
                this.notifyAll();                       // wake up threads sleeping in waitForPredicate() to notice exception
            }
        } catch (Throwable t) {
            this.log.error(this.name + " caught unexpected exception", t);
        } finally {
            synchronized (this) {
                this.thread = null;

                // Auto-restart if thread exits unexpectedly
                if (this.exception == null && isWorkOutstanding()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    this.log.warn(this.name + " auto-restart background thread");
                    wakeupAsyncThread();
                }
            }
        }
    }

    /**
     * Async writer thread main loop.
     */
    private void runLoop() throws IOException, InterruptedException {
        int nothingHappened = 0;
        while (true) {

            // Flush/close required?
            boolean needFlush = false;
            boolean needClose = false;
            synchronized (this) {
                needFlush = this.flushMark == 0;
                if (needFlush && this.closed)
                    needClose = true;
            }
            if (needFlush) {

                // Flush and (maybe) close
                this.out.flush();
                if (needClose)
                    this.out.close();

                // Update state
                synchronized (this) {
                    if (this.flushMark == 0) {
                        this.flushMark = -1;
                        this.notifyAll();               // wake up sleepers in waitForIdle()
                    }
                }
                nothingHappened = 0;
            }

            // Data to send?
            int wlen;
            synchronized (this) {
                wlen = this.count;
            }
            if (wlen > 0) {

                // Write data
                this.out.write(this.buf, 0, wlen);

                // Shift data in buffer
                synchronized (this) {
                    System.arraycopy(this.buf, wlen, this.buf, 0, this.count - wlen);
                    this.count -= wlen;
                    if (this.flushMark > 0)
                        this.flushMark -= wlen;
                    this.notifyAll();                // wake up sleepers in waitForSpace() and waitForIdle()
                }
                nothingHappened = 0;
                continue;
            }

            // Nothing was done. Did we linger already?
            if (++nothingHappened >= 2)
                break;

            // If nothing to do, linger a while before exiting
            synchronized (this) {
                this.wait(THREAD_LINGER_TIME);      // woken up by wakeupAsyncThread()
            }
        }
    }

    /**
     * Wait for some condition to become true. Of course somebody has to wake us up when it becomes true.
     */
    private synchronized boolean waitForPredicate(long timeout, final Predicate predicate)
      throws IOException, InterruptedException {
        try {
            return TimedWait.wait(this, timeout, new Predicate() {
                @Override
                public boolean test() {
                    try {
                        checkExceptions();
                    } catch (IOException e) {
                        throw new CheckedExceptionWrapper(e);
                    }
                    return predicate.test();
                }
            });
        } catch (CheckedExceptionWrapper e) {
            throw (IOException)e.getException();
        }
    }
}

