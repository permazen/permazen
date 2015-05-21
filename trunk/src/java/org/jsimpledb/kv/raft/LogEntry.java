
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsimpledb.kv.mvcc.Writes;

/**
 * Represents a Raft log entry.
 */
class LogEntry {

    public static final String LOG_FILE_PREFIX = "log-";
    public static final String LOG_FILE_SUFFIX = ".bin";
    public static final Pattern LOG_FILE_PATTERN = Pattern.compile(
      Pattern.quote(LOG_FILE_PREFIX) + "([0-9]{19})-([0-9]{19})" + Pattern.quote(LOG_FILE_SUFFIX));

    /**
     * Sorts instances by {@linkplain LogEntry#getIndex log index}.
     */
    public static final Comparator<LogEntry> SORT_BY_INDEX = new Comparator<LogEntry>() {
        @Override
        public int compare(LogEntry logEntry1, LogEntry logEntry2) {
            return Long.compare(logEntry1.getIndex(), logEntry2.getIndex());
        }
    };

    private final Timestamp createTime = new Timestamp();
    private final long term;
    private final long index;
    private final Writes writes;
    private final long writesSize;
    private final File file;

    private ByteBuffer content;

// Constructors

    /**
     * Constructor.
     *
     * @param term log entry term
     * @param index log entry index
     * @param logDir directory containing log files
     * @param writes deserialized writes
     * @param writesSize approximation of the size of {@code writes}
     */
    public LogEntry(long term, long index, File logDir, Writes writes, long writesSize) {
        Preconditions.checkArgument(term > 0, "bogus term");
        Preconditions.checkArgument(index > 0, "bogus index");
        Preconditions.checkArgument(logDir != null, "null logDir");
        Preconditions.checkArgument(writes != null, "null writes");
        this.term = term;
        this.index = index;
        this.writes = writes;
        this.writesSize = writesSize;
        this.file = new File(logDir, String.format("%s%019d-%019d%s",
          LOG_FILE_PREFIX, this.getIndex(), this.getTerm(), LOG_FILE_SUFFIX));
    }

// Properties

    /**
     * Get the age of this instance since instantiation.
     *
     * @return age in milliseconds
     */
    public int getAge() {
        return -this.createTime.offsetFromNow();
    }

    /**
     * Get the log term of this instance.
     *
     * @return associated log entry term
     */
    public long getTerm() {
        return this.term;
    }

    /**
     * Get the log index of this instance.
     *
     * @return associated log entry index
     */
    public long getIndex() {
        return this.index;
    }

    /**
     * Get the {@link Writes} associated with this entry.
     *
     * @return transaction writes
     */
    public Writes getWrites() {
        return this.writes;
    }

    public long getWritesSize() {
        return this.writesSize;
    }

    /**
     * Get the on-disk file for this log entry.
     *
     * @return log entry file
     */
    public File getFile() {
        return this.file;
    }

// File I/O

    /**
     * Get the serialized contents of this log entry by reading the file.
     */
    public ByteBuffer getContent() throws IOException {
        if (this.content == null)
            this.content = Util.readFile(this.getFile(), this.writesSize);
        return this.content.asReadOnlyBuffer();
    }

    /**
     * Extract the log term and index from the given on-disk log entry file name, if possible.
     *
     * @param file log file
     * @return {@link LogEntry} instance, or null if {@code file} or {@code file}'s name is invalid
     * @throws NullPointerException if {@code file} is null
     * @throws IOException if an I/O error occurs
     */
    public static LogEntry fromFile(File file) throws IOException {

        // Parse file name
        final Matcher matcher = LOG_FILE_PATTERN.matcher(file.getName());
        if (!matcher.matches())
            throw new IOException("invalid log file name `" + file.getName() + "'");
        final long index = Long.parseLong(matcher.group(1), 10);
        final long term = Long.parseLong(matcher.group(2), 10);

        // Get file length
        final long length = Util.getLength(file);

        // Read encoded mutations
        final Writes writes;
        try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file), 4096)) {
            writes = Writes.deserialize(input);
        }

        // Done
        return new LogEntry(term, index, file.getParentFile(), writes, length);
    }

// Object

    @Override
    public String toString() {
        return this.getTerm() + "/" + this.getIndex();
    }
}

