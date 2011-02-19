
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
 */
public class AsyncInputStream {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final InputStream input;
    private final String name;
    private final Listener listener;

    private final Thread thread;                // async reader thread
    private boolean closed;                     // this instance has been close()'d

    /**
     * Constructor.
     *
     * @param input     underlying input stream
     * @param name      name for this instance; used to create the name of the background thread
     * @param listener  callback object for input events
     * @throws IllegalArgumentException if any parameter is null
     */
    public AsyncInputStream(InputStream input, String name, Listener listener) {
        if (input == null)
            throw new IllegalArgumentException("null input");
        if (name == null)
            throw new IllegalArgumentException("name input");
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        this.input = input;
        this.name = name;
        this.listener = listener;
        this.thread = new Thread(this.name) {

            @Override
            public void run() {
                AsyncInputStream.this.threadMain();
            }
        };
        this.thread.start();
    }

    /**
     * Close this instance.
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
        } catch (IOException e) {
            try {
                this.listener.handleException(e);
            } catch (Exception e2) {
                this.log.error(this.name + ": caught unexpected exception", e2);
            }
        }
    }

    /**
     * Async reader thread main loop.
     */
    private void runLoop() throws IOException {
        byte[] buf = new byte[4096];
        while (true) {
            int r = this.input.read(buf);
            if (r == -1) {
                try {
                    this.listener.handleEOF();
                } catch (Exception e) {
                    this.log.error(this.name + ": caught unexpected exception", e);
                }
                break;
            }
            try {
                this.listener.handleInput(buf, 0, r);
            } catch (Exception e) {
                this.log.error(this.name + ": caught unexpected exception", e);
            }
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
         * Handle an {@link IOException} detected on the underlying input.
         * No further events will be delivered.
         *
         * @param e the exception received
         */
        void handleException(IOException e);

        /**
         * Handle end-of-file detected on the underlying input.
         * No further events will be delivered.
         */
        void handleEOF();
    }
}

