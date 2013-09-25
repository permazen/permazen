
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * Presents an {@link java.io.InputStream} interface given a {@link WriteCallback} that can write to an
 * {@link java.io.OutputStream}. A separate thread is created to perform the actual writing.
 *
 * @since 1.0.74
 */
public class NullModemInputStream extends FilterInputStream {

    private final PipedOutputStream output;

    /**
     * Constructor.
     *
     * <p>
     * The {@code writer}'s {@link WriteCallback#writeTo writeTo()} method will be invoked (once)
     * asynchronously in a dedicated writer thread. The {@link java.io.OutputStream} provided to it will
     * relay the bytes that are then read from this instance.
     * </p>
     *
     * @param writer    {@link java.io.OutputStream} writer callback
     * @param name      name for this instance; used to create the name of the background thread
     */
    public NullModemInputStream(final WriteCallback writer, String name) {
        super(new PipedInputStream());

        // Sanity check
        if (writer == null)
            throw new IllegalArgumentException("null writer");

        // Create other end of pipe
        try {
            this.output = new PipedOutputStream(this.getPipedInputStream());
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }

        // Launch writer thread
        Thread thread = new WriterThread(writer, this.output, name);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Get the wrapped stream cast as a {@link PipedInputStream}.
     */
    protected PipedInputStream getPipedInputStream() {
        return (PipedInputStream)this.in;
    }

    /**
     * Ensure input stream is closed when this instance is no longer referenced.
     *
     * <p>
     * This ensures the writer thread wakes up (and exits, avoiding a memory leak) when an instance of this class
     * is created but never read from.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            try {
                this.getPipedInputStream().close();
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

        private final WriteCallback writer;
        private final PipedOutputStream output;

        WriterThread(WriteCallback writer, PipedOutputStream output, String name) {
            super(name);
            this.writer = writer;
            this.output = output;
        }

        @Override
        public void run() {
            try {
                this.writer.writeTo(this.output);
            } catch (IOException e) {
                // ignore - reader will get another IOException because pipe is about to be broken
            } finally {
                try {
                    this.output.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}

