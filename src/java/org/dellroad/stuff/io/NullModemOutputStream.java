
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * Presents an {@link java.io.OutputStream OutputStream} interface given a {@link ReadCallback} that can read from an
 * {@link InputStream}. A separate thread is created to perform the actual reading.
 *
 * <p>
 * If data is written beyond what the reader is willing to consume, an {@link IOException} is thrown.
 *
 * @since 1.0.82
 */
public class NullModemOutputStream extends FilterOutputStream {

    /**
     * Constructor.
     *
     * @param reader    {@link InputStream} reader callback
     * @param name      name for this instance; used to create the name of the background thread
     */
    public NullModemOutputStream(final ReadCallback reader, String name) {
        super(new PipedOutputStream());

        // Sanity check
        if (reader == null)
            throw new IllegalArgumentException("null reader");

        // Create other end of pipe
        PipedInputStream input;
        try {
            input = new PipedInputStream(this.getPipedOutputStream());
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }

        // Launch reader thread
        Thread thread = new ReaderThread(reader, input, name);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Get the wrapped stream cast as a {@link PipedOutputStream}.
     */
    protected PipedOutputStream getPipedOutputStream() {
        return (PipedOutputStream)this.out;
    }

    /**
     * Ensure output stream is closed when this instance is no longer referenced.
     *
     * <p>
     * This ensures the reader thread wakes up (and exits, avoiding a memory leak) when an instance of this class
     * is created but never read from.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            try {
                this.getPipedOutputStream().close();
            } catch (IOException e) {
                // ignore
            }
        } finally {
            super.finalize();
        }
    }

    /**
     * Callback interface used by {@link NullModemOutputStream}.
     */
    public interface ReadCallback {

        /**
         * Read from the given input stream.
         *
         * <p>
         * This method will be invoked (once) asynchronously in a dedicated reader thread.
         * </p>
         *
         * @param input input providing the data written to the corresponding {@link NullModemOutputStream}
         * @throws IOException if an I/O error occurs
         */
        void readFrom(InputStream input) throws IOException;
    }

    /**
     * Reader thread. This is designed to not hold a reference to the {@link NullModemOutputStream}.
     */
    private static class ReaderThread extends Thread {

        private final ReadCallback reader;
        private final PipedInputStream input;

        ReaderThread(ReadCallback reader, PipedInputStream input, String name) {
            super(name);
            this.reader = reader;
            this.input = input;
        }

        @Override
        public void run() {
            try {
                this.reader.readFrom(this.input);
            } catch (IOException e) {
                // ignore - writer will get another IOException because pipe is about to be broken
            } finally {
                try {
                    this.input.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}

