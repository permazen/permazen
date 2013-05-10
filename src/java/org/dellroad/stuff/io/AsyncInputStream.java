
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs asynchonous reads on an {@link InputStream} and notifies of input events.
 *
 * <p>
 * Reads are performed in a dedicated background thread, from which the configured listener is notified.
 * The background thread runs until this instance is {@linkplain #close closed}, EOF or an exception is detected
 * on the input, or a listener callback method throws an exception. A null listener may be supplied; in which case
 * this class will just sink the {@link InputStream}.
 * </p>
 *
 * <p>
 * Instances of this class are thread-safe. The {@link #close} method may be safely invoked re-entrantly from the
 * listener callback methods.
 * </p>
 */
public class AsyncInputStream {

    private static final int BUFFER_SIZE = 4096;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final InputStream input;
    private final String name;
    private final Listener listener;

    private boolean closed;                     // this instance has been close()'d

    /**
     * Constructor.
     *
     * <p>
     * If {@code listener} is null, this instance effectively reads and discards all of the input in a background thread.
     * </p>
     *
     * @param input     underlying input stream
     * @param name      name for this instance; used to create the name of the background thread
     * @param listener  callback object for input events, or null for none
     * @throws IllegalArgumentException if any parameter is null
     */
    public AsyncInputStream(InputStream input, String name, Listener listener) {
        if (input == null)
            throw new IllegalArgumentException("null input");
        if (name == null)
            throw new IllegalArgumentException("name input");
        this.input = input;
        this.name = name;
        this.listener = listener;
        new Thread(this.name) {
            @Override
            public void run() {
                AsyncInputStream.this.threadMain();
            }
        }.start();
    }

    /**
     * Close this instance.
     *
     * <p>
     * Does nothing if already closed.
     */
    public synchronized void close() {
        if (this.closed)
            return;
        try {
            this.input.close();
        } catch (IOException e) {
            // ignore; we assume main thread will awake in any case
        }
        this.closed = true;
    }

    /**
     * Writer thread main entry point.
     */
    private void threadMain() {
        try {
            this.runLoop();
        } catch (Throwable t) {
            synchronized (this) {
                if (this.closed)
                    return;
            }
            try {
                if (this.listener != null)
                    this.listener.handleException(t);
            } catch (Exception e) {
                this.log.error(this.name + ": caught unexpected exception", e);
            }
        }
    }

    /**
     * Async reader thread main loop.
     */
    private void runLoop() throws IOException {
        byte[] buf = new byte[BUFFER_SIZE];
        while (true) {
            int r = this.input.read(buf);
            if (r == -1) {
                if (this.listener != null)
                    this.listener.handleEOF();
                break;
            }
            if (this.listener != null)
                this.listener.handleInput(buf, 0, r);
        }
    }

    /**
     * Callback interface required by {@link AsyncInputStream}.
     */
    public interface Listener {

        /**
         * Handle new data read from the underlying input.
         * This method must not write to buffer bytes outside of the defined region.
         *
         * @param buf data buffer
         * @param off starting offset of data in buffer
         * @param len number of bytes of data
         */
        void handleInput(byte[] buf, int off, int len);

        /**
         * Handle an exception detected on the underlying input.
         * No further events will be delivered.
         *
         * <p>
         * Typically the assocaited {@link AsyncInputStream} will be closed in this callback.
         *
         * @param e the exception received (usually {@link IOException} but could also be any other {@link RuntimeException})
         */
        void handleException(Throwable e);

        /**
         * Handle end-of-file detected on the underlying input.
         * No further events will be delivered.
         *
         * <p>
         * Typically the assocaited {@link AsyncInputStream} will be closed in this callback.
         */
        void handleEOF();
    }
}

