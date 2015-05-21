
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

/**
 * Utility class to write files, counting the number of bytes written, and {@code fsync()}'ing the file on {@link #close}.
 */
class FileWriter extends FilterOutputStream {

    private final File file;
    private final FileOutputStream fileOutput;

    private long length;

    /**
     * Constructor
     *
     * @param file file to write
     * @throws IOException if an error occurs opening {@code file}
     * @throws IllegalArgumentException if {@code file} is null
     */
    public FileWriter(File file) throws IOException {
        super(null);
        Preconditions.checkArgument(file != null, "null file");
        this.file = file;
        this.fileOutput = new FileOutputStream(file);
        this.out = new BufferedOutputStream(this.fileOutput, 4096);
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

    @Override
    public void close() throws IOException {
        this.out.flush();
        this.fileOutput.getChannel().force(false);
        this.out.close();
    }
}

