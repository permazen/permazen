
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

/**
 * Utility class for writing files, counting the number of bytes written, and optionally
 * {@code fsync()}'ing the file automatically on {@link #close}.
 */
class FileWriter extends FilterOutputStream {

    private final File file;
    private final FileOutputStream fileOutput;
    private final boolean disableSync;

    private long length;

    /**
     * Constructor
     *
     * @param file file to write
     * @param disableSync true to disable automatic data sync on close
     * @throws IOException if an error occurs opening {@code file}
     * @throws IllegalArgumentException if {@code file} is null
     */
    FileWriter(final File file, final boolean disableSync) throws IOException {
        super(null);
        Preconditions.checkArgument(file != null, "null file");
        this.file = file;
        this.fileOutput = new FileOutputStream(file);
        this.out = new BufferedOutputStream(this.fileOutput, 4096);
        this.disableSync = disableSync;
    }

    /**
     * Get the {@link File} we're writing to.
     *
     * @return written file
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Get the underlying {@link FileOutputStream}.
     *
     * @return file output stream
     */
    public FileOutputStream getFileOutputStream() {
        return this.fileOutput;
    }

    /**
     * Get the number of bytes written so far.
     *
     * @return number of bytes written
     */
    public long getLength() {
        return this.length;
    }

// FilterOutputStream

    @Override
    public void write(int value) throws IOException {
        this.out.write(value);
        this.length++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.out.write(b, off, len);
        this.length += len;
    }

    /**
     * Closes this output stream and releases any system resources associated with the stream.
     *
     * <p>
     * If this instance is so configured, the file's content will also be durably persisted via
     * {@link java.nio.channels.FileChannel#force FileChannel.force(false)} before this method returns.
     *
     * @throws IOException {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.out.flush();
        if (!this.disableSync)
            this.fileOutput.getChannel().force(false);
        this.out.close();
    }
}
