
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * A {@link FileOutputStream} that atomically updates the target file.
 *
 * <p>
 * Instances write to a temporary file until {@link #close} is invoked, at which time the temporary file
 * gets {@linkplain File#renameTo renamed} to the target file. This rename operation is atomic on most systems
 * (e.g., all UNIX variants). The result is that the target file always exists, and if opened at any time,
 * will contain either the previous content or the new content, but never a mix of the two.
 *
 * <p>
 * An open instance can be thought of as representing an open transaction to rewrite the file.
 * The "transaction" is committed via {@link #close}, or may be aborted via {@link #cancel} (which
 * deletes the temporary file).
 */
public class AtomicUpdateFileOutputStream extends FileOutputStream {

    private final File targetFile;

    private File tempFile;
    private long timestamp;

    /**
     * Constructor.
     *
     * @param targetFile the ultimate destination for the output when {@linkplain #close closed}.
     * @param tempFile temporary file that accumulates output until {@linkplain #close close}.
     * @throws FileNotFoundException if {@code tempFile} cannot be opened for any reason
     * @throws SecurityException if a security manager prevents writing to {@code tempFile}
     * @throws NullPointerException if either parameter is null
     */
    public AtomicUpdateFileOutputStream(File targetFile, File tempFile) throws FileNotFoundException {
        super(tempFile);
        this.tempFile = tempFile;
        if (targetFile == null)
            throw new NullPointerException("null targetFile");
        this.targetFile = targetFile;
    }

    /**
     * Get the target file.
     *
     * @return target file, never null
     */
    public synchronized File getTargetFile() {
        return this.targetFile;
    }

    /**
     * Get the temporary file.
     *
     * <p>
     * If this instance has already been {@linkplain #close closed} (either successfully or not)
     * or {@linkplain #cancel canceled}, this will return null.
     *
     * @return temporary file, or null if {@link #close} or {@link #cancel} has already been invoked
     */
    public synchronized File getTempFile() {
        return this.tempFile;
    }

    /**
     * Cancel this instance. This "aborts" the open "transaction", and deletes the temporary file.
     *
     * <p>
     * Does nothing if {@link #close} or {@link #cancel} has already been invoked.
     */
    public synchronized void cancel() {
        if (this.tempFile != null) {
            this.tempFile.delete();
            this.tempFile = null;
        }
    }

    /**
     * Close this instance. This "commits" the open "transaction".
     *
     * <p>
     * If successful, the configured {@code tempFile} will be {@linkplain File#renameTo renamed}
     * to the configured destination file {@code targetFile}. In any case, after this method returns
     * (either normally or abnormally), the temporary file will be deleted.
     *
     * @throws IOException if {@link #close} or {@link #cancel} has already been invoked
     */
    @Override
    public synchronized void close() throws IOException {

        // Sanity check
        if (this.tempFile == null)
            throw new IOException("already closed or canceled");

        // Close temporary file
        super.close();

        // Read updated modification time
        final long newTimestamp = this.tempFile.lastModified();

        // Rename file, or delete it if that fails
        try {
            if (!this.tempFile.renameTo(this.targetFile)) {
                throw new IOException("error renaming temporary file `" + this.tempFile.getName()
                  + "' to `" + this.targetFile.getName() + "'");
            }
            this.tempFile = null;
        } finally {
            if (this.tempFile != null)          // exception thrown, cancel transaction
                this.cancel();
        }

        // Update target file timestamp
        this.timestamp = newTimestamp;
    }

    /**
     * Get the last modification timestamp of the target file as it was at the time it was updated by this instance.
     *
     * <p>
     * This method only works after {@link #close} has been successfully invoked, otherwise it returns zero.
     *
     * @return target file modification time, or zero if {@link #close} has not been successfully invoked
     */
    public synchronized long getTimestamp() {
        return this.timestamp;
    }

    /**
     * Ensure the temporary file is deleted in cases where this instance never got successfully closed.
     */
    @Override
    protected void finalize() throws IOException {
        try {
            if (this.tempFile != null)
                this.cancel();
        } finally {
            super.finalize();
        }
    }
}
