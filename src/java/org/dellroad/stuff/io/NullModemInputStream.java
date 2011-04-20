
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * Presents an {@link InputStream} interface given a callback that can write to an {@link OutputStream}.
 * A separate thread is created to perform the actual writing.
 *
 * @since 1.0.74
 */
public class NullModemInputStream extends PipedInputStream {

    private final PipedOutputStream output;

    /**
     * Constructor.
     *
     * @param writer    {@link OutputStream} writer callback
     * @param name      name for this instance; used to create the name of the background thread
     */
    public NullModemInputStream(final CallableWriter writer, String name) {

        // Sanity check
        if (writer == null)
            throw new IllegalArgumentException("null writer");

        // Create output stream
        try {
            this.output = new PipedOutputStream(this);
        } catch (IOException e) {
            throw new RuntimeException("impossible case", e);
        }

        // Launch writer thread
        Thread thread = new WriterThread(writer, this.output, name);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Callback interface used by {@link NullModemInputStream}.
     */
    public interface CallableWriter {

        /**
         * Write the output to the given output stream.
         *
         * <p>
         * This method will be invoked asynchronously by a dedicated writer thread.
         * </p>
         *
         * @throws IOException if an I/O error occurs
         */
        void writeTo(OutputStream output) throws IOException;
    }

    /**
     * Ensure input and output streams are closed when this instance is no longer referenced.
     *
     * <p>
     * This avoids a memory leak when an instance of this class is created but never read from.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            try {
                this.output.close();
            } catch (IOException e) {
                // ignore
            }
            try {
                this.close();
            } catch (IOException e) {
                // ignore
            }
        } finally {
            super.finalize();
        }
    }

    /**
     * Writer thread. This is designed to not hold a reference to the {@link NullModemInputStream}.
     */
    private static class WriterThread extends Thread {

        private final CallableWriter writer;
        private final PipedOutputStream output;

        WriterThread(CallableWriter writer, PipedOutputStream output, String name) {
            super(name);
            this.writer = writer;
            this.output = output;
        }

        @Override
        public void run() {
            try {
                this.writer.writeTo(this.output);
                this.output.close();
            } catch (IOException e) {
                // ignore - reader will get another IOException because pipe is about to be broken
            }
        }
    }
}

