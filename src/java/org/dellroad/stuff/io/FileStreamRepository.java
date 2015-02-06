
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a file-based implementation of the {@link StreamRepository} interface with
 * the added feature of automated backups.
 *
 * <p>
 * Atomic updates are implemented using an {@link AtomicUpdateFileOutputStream}.
 * </p>
 *
 * <p>
 * When backups are configured, the base file must be copied, not moved, to the first backup on update to avoid
 * a small window where the base file doesn't exist. This class uses {@linkplain Files#createLink hard links} to perform
 * this "copy" efficiently. This behavior can be altered by overriding {@link #copy copy()} on systems not supporting
 * hard links.
 * </p>
 */
public class FileStreamRepository implements StreamRepository {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final File file;

    private int numBackups;
    private long timestamp;

    /**
     * Primary constructor.
     *
     * @param file the file that will store the stream content
     * @param numBackups number of backup copies to keep
     * @throws IllegalArgumentException if {@code file} is null
     * @throws IllegalArgumentException if {@code numBackups} is negative
     */
    public FileStreamRepository(File file, int numBackups) {
        if (file == null)
            throw new IllegalArgumentException("null file");
        if (numBackups < 0)
            throw new IllegalArgumentException("negative numBackups");
        this.file = file;
        this.numBackups = numBackups;
    }

    /**
     * Convenience constructor for the case where no backup copies are needed.
     *
     * <p>
     * Equivalent to:
     * <blockquote><code>
     *  FileStreamRepository(file, 0);
     * </code></blockquote>
     */
    public FileStreamRepository(File file) {
        this(file, 0);
    }

    /**
     * Get the configured {@link File}.
     *
     * @return the file that stores the stream content (same as given to constructor)
     */
    public final File getFile() {
        return this.file;
    }

    /**
     * Get the configured number of backup files.
     *
     * @return the number of backup files to maintain
     */
    public final int getNumBackups() {
        return this.numBackups;
    }

    /**
     * Change the number of backup files to maintain.
     *
     * <p>
     * If the number of backups is reduced, the "extra" backup files are not touched.
     *
     * @param numBackups number of backup files
     * @throws IllegalArgumentException if {@code numBackups} is less than zero
     */
    public void setNumBackups(int numBackups) {
        this.numBackups = numBackups;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new FileInputStream(this.file);
    }

    @Override
    public AtomicUpdateFileOutputStream getOutputStream() throws IOException {
        String tempName = this.file.getName();
        while (tempName.length() < 3)
            tempName += "z";
        final File tempFile = File.createTempFile(tempName, null, this.file.getParentFile());
        return new AtomicUpdateFileOutputStream(this.file, tempFile) {
            @Override
            public void close() throws IOException {
                synchronized (FileStreamRepository.this) {

                    // Rotate backups
                    FileStreamRepository.this.rotateBackups();

                    // Save and update file
                    super.close();

                    // Update timestamp
                    FileStreamRepository.this.timestamp = this.getTimestamp();
                }
            }
        };
    }

    /**
     * Get the last modification timestamp of the target file as known to this instance.
     *
     * <p>
     * This returns the modification timestamp as known by this instance; it does not ask the filesystem
     * for the actual last modification timestamp. So if the two are different, then the file has
     * been updated by some external process. In other words, this returns the modification timestamp
     * of the underlying file as it was at the time of the most recent update.
     *
     * @return file's last modification timestamp as known by this instance,
     *  or zero if the file has not yet been successfully written to
     */
    public synchronized long getFileTimestamp() {
        return this.timestamp;
    }

    /**
     * Generate a backup file name.
     *
     * <p>
     * The implementation in {@link FileStreamRepository} returns a file with the same name as {@code file}
     * plus a suffix <code>.1</code>,  <code>.2</code>, <code>.3</code>, etc. corresponding to {@code index}.
     * Subclasses may override as desired.
     *
     * @param file the file that stores the current stream content (i.e., from {@link #getFile})
     * @param index backup index, always greater than or equal to 1
     */
    protected File getBackupFile(File file, int index) {
        return new File(file.toString() + "." + index);
    }

    /**
     * Copy, via hard link if possible, a file. If the two files are the same, nothing should be done.
     *
     * <p>
     * The implementation in {@link FileStreamRepository} uses {@linkplain Files#createLink hard links}.
     * Subclasses must override this method if the platform does not support hard links.
     * </p>
     *
     * @param src source file
     * @param dst destination file
     * @throws IOException if unsuccessful
     */
    protected void copy(File src, File dst) throws IOException {
        final Path srcPath = src.toPath();
        final Path dstPath = dst.toPath();
        try {
            if (dst.exists() && Files.isSameFile(srcPath, dstPath))
                return;
        } catch (IOException e) {
            // ignore
        }
        try {
            Files.createLink(dstPath, srcPath);
        } catch (FileAlreadyExistsException e) {
            dst.delete();
            Files.createLink(dstPath, srcPath);
        }
    }

    /**
     * Rotate backup files.
     */
    private synchronized void rotateBackups() throws IOException {

        // Generate file names: 0 -> "file", 1 -> "file.1", 2 -> "file.2", etc.
        File[] files = new File[this.numBackups + 1];
        files[0] = this.getFile();
        for (int i = 0; i < this.numBackups; i++)
            files[i + 1] = this.getBackupFile(this.getFile(), i + 1);

        // Rotate backups
        for (int i = this.numBackups - 1; i >= 0; i--) {
            final File src = files[i];
            final File dst = files[i + 1];

            // For the first backup, create a copy so there's no window when the target file does not exist
            if (i == 0) {
                try {
                    this.copy(src, dst);
                } catch (IOException e) {
                    this.log.warn("failed to copy `" + src + "' to backup `" + dst + "'", e);
                }
            } else
                src.renameTo(dst);      // OK if this fails, that means the backup file doesn't exist yet
        }
    }
}

