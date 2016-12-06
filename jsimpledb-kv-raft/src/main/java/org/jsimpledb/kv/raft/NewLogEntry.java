
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import java.io.File;
import java.io.IOException;

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
     * @param tempFile temporary file containing serialized mutations
     * @throws Exception if an error occurs
     */
    NewLogEntry(RaftKVTransaction tx, File tempFile) throws IOException {
        assert tx != null;
        assert tempFile != null;
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
        assert tx != null;
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
        assert raft != null;
        assert data != null;
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

