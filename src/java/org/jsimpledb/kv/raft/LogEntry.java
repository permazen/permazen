
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
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
    private final String[] configChange;
    private final long fileSize;
    private final File file;

    private ByteBuffer content;

// Constructors

    /**
     * Constructor.
     *
     * @param term log entry term
     * @param index log entry index
     * @param logDir directory containing log files
     * @param data log entry data
     * @param fileSize the size of the file
     */
    public LogEntry(long term, long index, File logDir, Data data, long fileSize) {
        Preconditions.checkArgument(term > 0, "bogus term");
        Preconditions.checkArgument(index > 0, "bogus index");
        Preconditions.checkArgument(logDir != null, "null logDir");
        Preconditions.checkArgument(data != null, "null data");
        Preconditions.checkArgument(fileSize > 0, "invalid fileSize");
        this.term = term;
        this.index = index;
        this.writes = data.getWrites();
        this.configChange = data.getConfigChange();
        this.fileSize = fileSize;
        this.file = new File(logDir,
          String.format("%s%019d-%019d%s", LOG_FILE_PREFIX, this.getIndex(), this.getTerm(), LOG_FILE_SUFFIX));
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

    /**
     * Get the cluster config change associated with this log entry, if any.
     *
     * @return cluster config change, or null if none
     */
    public String[] getConfigChange() {
        return this.configChange;
    }

    /**
     * Apply the cluster config change associated with this log entry, if any.
     *
     * @param config configuration to modify
     * @return true if {@code config} was modified, otherwise false
     */
    public boolean applyConfigChange(Map<String, String> config) {
        if (this.configChange == null)
            return false;
        return this.configChange[1] != null ?
          !this.configChange[1].equals(config.put(this.configChange[0], this.configChange[1])) :
          config.remove(this.configChange[0]) != null;
    }

    /**
     * Get the size of the on-disk file for this log entry.
     *
     * @return log entry file size
     */
    public long getFileSize() {
        return this.fileSize;
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
            this.content = Util.readFile(this.getFile(), this.fileSize);
        return this.content.asReadOnlyBuffer();
    }

    /**
     * Create a {@link LogEntry} from the specified file.
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
        final long fileLength = Util.getLength(file);

        // Decode log entry
        final Data data;
        try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file), 4096)) {
            data = LogEntry.readData(input);
        }

        // Done
        return new LogEntry(term, index, file.getParentFile(), data, fileLength);
    }

    /**
     * Read log entry data from the specified input.
     *
     * @param input input stream
     * @return log entry data
     * @throws IOException if an I/O error occurs
     */
    public static Data readData(InputStream input) throws IOException {
        Preconditions.checkArgument(input != null, "null input");

        // Get writes
        final Writes writes = Writes.deserialize(input);

        // Get config change, if any
        final String[] configChange;
        final DataInputStream data = new DataInputStream(input);
        if (data.readBoolean()) {
            configChange = new String[2];
            configChange[0] = data.readUTF();
            if (data.readBoolean())
                configChange[1] = data.readUTF();
        } else
            configChange = null;

        // Verify end of file
        if (input.read() != -1)
            throw new IOException("log entry input contains trailing garbage");

        // Done
        return new Data(writes, configChange);
    }

    /**
     * Write log entry data to the specified output.
     *
     * @param output output stream
     * @param data log entry data
     * @throws IOException if an I/O error occurs
     */
    public static void writeData(OutputStream output, Data data) throws IOException {
        Preconditions.checkArgument(output != null, "null output");
        Preconditions.checkArgument(data != null, "null data");
        data.getWrites().serialize(output);
        final DataOutputStream dataOutput = new DataOutputStream(output);
        final String[] configChange = data.getConfigChange();
        dataOutput.writeBoolean(configChange != null);
        if (configChange != null) {
            dataOutput.writeUTF(configChange[0]);
            dataOutput.writeBoolean(configChange[1] != null);
            if (configChange[1] != null)
                dataOutput.writeUTF(configChange[1]);
        }
        dataOutput.flush();
    }

// Object

    @Override
    public String toString() {
        return this.getIndex() + "t" + this.getTerm()
        + (this.configChange != null ?
            (this.configChange[1] != null ?
              "+" + this.configChange[0] + "@" + this.configChange[1] :
              "-" + this.configChange[0]) : "");
    }

// Data

    /**
     * Data associated with a {@link LogEntry}.
     */
    public static class Data {

        private final Writes writes;
        private final String[] configChange;

        /**
         * Constructor.
         *
         * @param writes key/value mutations
         * @param configChange cluster config change (identity, address), or null for none
         */
        public Data(Writes writes, String[] configChange) {
            Preconditions.checkArgument(writes != null, "null writes");
            Preconditions.checkArgument(configChange == null || (configChange.length == 2 && configChange[0] != null));
            this.writes = writes;
            this.configChange = configChange;
        }

        public Writes getWrites() {
            return this.writes;
        }

        public String[] getConfigChange() {
            return this.configChange;
        }
    }
}

