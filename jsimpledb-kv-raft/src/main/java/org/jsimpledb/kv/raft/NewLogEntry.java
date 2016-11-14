
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Contains the information required to commit a new entry to the log.
 */
class NewLogEntry {

    private final LogEntry.Data data;
    private final File tempFile;

    /**
     * Create an instance from a transaction and a temporary file.
     *
     * @param data log entry mutations
     * @throws Exception if an error occurs
     */
    NewLogEntry(RaftKVTransaction tx, File tempFile) throws IOException {
        this.data = new LogEntry.Data(tx.view.getWrites(), tx.getConfigChange());
        this.tempFile = tempFile;
    }

    /**
     * Create an instance from a transaction.
     *
     * @param data log entry mutations
     * @throws Exception if an error occurs
     */
    NewLogEntry(RaftKVTransaction tx) throws IOException {
        this.data = new LogEntry.Data(tx.view.getWrites(), tx.getConfigChange());
        this.tempFile = File.createTempFile(RaftKVDatabase.TEMP_FILE_PREFIX, RaftKVDatabase.TEMP_FILE_SUFFIX, tx.raft.logDir);
        boolean success = false;
        try (FileWriter output = new FileWriter(this.tempFile, tx.raft.disableSync)) {
            LogEntry.writeData(output, data);
            success = true;
        } finally {
            if (!success)
                Util.delete(this.tempFile, "new log entry temp file");
        }
    }

    /**
     * Create an instance from a {@link LogEntry.Data} object.
     *
     * @param data mutation data
     * @throws Exception if an error occurs
     */
    NewLogEntry(RaftKVDatabase raft, LogEntry.Data data) throws IOException {
        this.data = data;
        this.tempFile = File.createTempFile(RaftKVDatabase.TEMP_FILE_PREFIX, RaftKVDatabase.TEMP_FILE_SUFFIX, raft.logDir);
        boolean success = false;
        try (FileWriter output = new FileWriter(this.tempFile, raft.disableSync)) {
            LogEntry.writeData(output, data);
            success = true;
        } finally {
            if (!success)
                Util.delete(this.tempFile, "new log entry temp file");
        }
    }

    /**
     * Create an instance from a serialized data in a {@link ByteBuffer}.
     *
     * @param buf buffer containing serialized mutations
     * @throws Exception if an error occurs
     */
    NewLogEntry(RaftKVDatabase raft, ByteBuffer dataBuf) throws IOException {

        // Create temporary file
        this.tempFile = File.createTempFile(RaftKVDatabase.TEMP_FILE_PREFIX, RaftKVDatabase.TEMP_FILE_SUFFIX, raft.logDir);
        boolean success = false;
        try {

            // Copy data to temporary file
            try (FileWriter output = new FileWriter(this.tempFile, raft.disableSync)) {
                while (dataBuf.hasRemaining())
                    output.getFileOutputStream().getChannel().write(dataBuf);
            }

            // Avoid having two copies of the data in memory at once
            dataBuf = null;

            // Deserialize data from file back into memory
            try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(tempFile), 4096)) {
                this.data = LogEntry.readData(input);
            }
            success = true;
        } finally {
            if (!success)
                Util.delete(this.tempFile, "new log entry temp file");
        }
    }

    public LogEntry.Data getData() {
        return this.data;
    }

    public File getTempFile() {
        return this.tempFile;
    }

    public void cancel() {
        Util.delete(this.tempFile, "new log entry temp file");
    }
}

