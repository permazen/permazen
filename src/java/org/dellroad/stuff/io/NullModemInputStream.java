
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Presents an {@link InputStream} interface given a callback that can write to an {@link OutputStream}.
 * A separate thread is created to perform the actual writing.
 *
 * @since 1.0.74
 */
public class NullModemInputStream extends PipedInputStream {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final CallableWriter writer;
    private final PipedOutputStream output;

    /**
     * Constructor.
     *
     * @param writer    {@link OutputStream} writer callback
     * @param name      name for this instance; used to create the name of the background thread
     */
    public NullModemInputStream(CallableWriter writer, String name) {
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        this.writer = writer;
        try {
            this.output = new PipedOutputStream(this);
        } catch (IOException e) {
            throw new RuntimeException("impossible case", e);
        }
        Thread thread = new Thread(name) {
            @Override
            public void run() {
                NullModemInputStream.this.threadMain();
            }
        };
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Writer thread main entry point.
     */
    private void threadMain() {
        try {
            this.writer.writeTo(this.output);
            this.output.close();
        } catch (IOException e) {
            // ignore - reader will get another IOException because pipe is about to be broken
        }
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
}

