
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;

/**
 * Contains the information required to commit a new entry to the log.
 *
 * <p>
 * Instances must be {@link #close}'ed when no longer needed to ensure the temporary file is deleted if not used.
 */
class NewLogEntry {

    private final LogEntry.Data data;
    private final File tempFile;

    private boolean tempFileReset;

    /**
     * Create an instance from a local transaction and an existing temporary file.
     *
     * @param tx local transaction
     * @param tempFile temporary file containing serialized mutations
     */
    NewLogEntry(RaftKVTransaction tx, File tempFile) throws IOException {
        this(new LogEntry.Data(tx.view.getWrites(), tx.getConfigChange()), tempFile);
    }

    /**
     * Create an instance from a local transaction. A corresponding temporary file will be created automatically.
     *
     * @param tx local transaction
     */
    NewLogEntry(RaftKVTransaction tx) throws IOException {
        this(tx.raft, new LogEntry.Data(tx.view.getWrites(), tx.getConfigChange()));
    }

    /**
     * Create an instance from a {@link LogEntry.Data} object. A corresponding temporary file will be created automatically.
     *
     * @param raft database
     * @param data mutation data
     * @throws IOException if an I/O error occurs
     */
    NewLogEntry(RaftKVDatabase raft, LogEntry.Data data) throws IOException {
        this(data, NewLogEntry.writeDataToFile(data, raft, raft.disableSync));
    }

    /**
     * Create an instance from a {@link LogEntry.Data} object and an existing temporary file.
     *
     * @param data mutation data
     * @param tempFile temporary file containing serialized mutations
     */
    NewLogEntry(LogEntry.Data data, File tempFile) {
        assert data != null;
        assert tempFile != null;
        this.data = data;
        this.tempFile = tempFile;
    }

    public LogEntry.Data getData() {
        return this.data;
    }

    public File getTempFile() {
        Preconditions.checkState(!this.tempFileReset);
        return this.tempFile;
    }

    public void resetTempFile() {
        assert !this.tempFile.exists();
        this.tempFileReset = true;
    }

    public void cleanup(RaftKVDatabase raft) {
        if (!this.tempFileReset)
            raft.deleteFile(this.tempFile, "new log entry temp file");
    }

    private static File writeDataToFile(LogEntry.Data data, RaftKVDatabase raft, boolean disableSync) throws IOException {
        final File tempFile = raft.getTempFile();
        boolean success = false;
        try (FileWriter output = new FileWriter(tempFile, disableSync)) {
            LogEntry.writeData(output, data);
            success = true;
        } finally {
            if (!success)
                raft.deleteFile(tempFile, "new log entry temp file");
        }
        return tempFile;
    }
}

