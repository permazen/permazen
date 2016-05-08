
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.jsimpledb.util.LongEncoder;

/**
 * Sent from leader to follower to with a chunk of key/value pairs that will wholesale replace the follower's key/value store.
 */
public class InstallSnapshot extends Message {

    private final long snapshotTerm;
    private final long snapshotIndex;
    private final Map<String, String> snapshotConfig;
    private final long pairIndex;
    private final boolean lastChunk;
    private final ByteBuffer data;

// Constructors

    /**
     * Constructor.
     *
     * @param clusterId cluster ID
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param snapshotTerm term of the last log entry in the snapshot
     * @param snapshotIndex index of the last log entry in the snapshot
     * @param snapshotConfig cluster config of the last log entry in the snapshot (first {@code pairIndex} only)
     * @param pairIndex index of the first key/value pair in this chunk
     * @param lastChunk true if this is the last chunk in the snapshot
     * @param data encoded key/value pairs
     */
    public InstallSnapshot(int clusterId, String senderId, String recipientId, long term, long snapshotTerm,
      long snapshotIndex, long pairIndex, Map<String, String> snapshotConfig, boolean lastChunk, ByteBuffer data) {
        super(Message.INSTALL_SNAPSHOT_TYPE, clusterId, senderId, recipientId, term);
        this.snapshotTerm = snapshotTerm;
        this.snapshotIndex = snapshotIndex;
        this.pairIndex = pairIndex;
        this.snapshotConfig = snapshotConfig;
        this.lastChunk = lastChunk;
        this.data = data;
        this.checkArguments();
    }

    InstallSnapshot(ByteBuffer buf) {
        super(Message.INSTALL_SNAPSHOT_TYPE, buf);
        this.snapshotTerm = LongEncoder.read(buf);
        this.snapshotIndex = LongEncoder.read(buf);
        this.pairIndex = LongEncoder.read(buf);
        this.snapshotConfig = this.pairIndex == 0 ? InstallSnapshot.getSnapshotConfig(buf) : null;
        this.lastChunk = Message.getBoolean(buf);
        this.data = Message.getByteBuffer(buf);
        this.checkArguments();
    }

    @Override
    void checkArguments() {
        super.checkArguments();
        Preconditions.checkArgument(this.snapshotTerm > 0);
        Preconditions.checkArgument(this.snapshotIndex > 0);
        Preconditions.checkArgument(this.pairIndex >= 0);
        Preconditions.checkArgument((this.pairIndex == 0) == (this.snapshotConfig != null));
        Preconditions.checkArgument(this.data != null);
    }

// Properties

    public long getSnapshotTerm() {
        return this.snapshotTerm;
    }

    public long getSnapshotIndex() {
        return this.snapshotIndex;
    }

    public Map<String, String> getSnapshotConfig() {
        return this.snapshotConfig;
    }

    public long getPairIndex() {
        return this.pairIndex;
    }

    public boolean isLastChunk() {
        return this.lastChunk;
    }

    public ByteBuffer getData() {
        return this.data.asReadOnlyBuffer();
    }

// Message

    @Override
    public boolean isLeaderMessage() {
        return true;
    }

    @Override
    public void visit(MessageSwitch handler) {
        handler.caseInstallSnapshot(this);
    }

    @Override
    public void writeTo(ByteBuffer dest) {
        super.writeTo(dest);
        LongEncoder.write(dest, this.snapshotTerm);
        LongEncoder.write(dest, this.snapshotIndex);
        LongEncoder.write(dest, this.pairIndex);
        if (this.pairIndex == 0)
            InstallSnapshot.putSnapshotConfig(dest, this.snapshotConfig);
        Message.putBoolean(dest, this.lastChunk);
        Message.putByteBuffer(dest, this.data);
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + LongEncoder.encodeLength(this.snapshotTerm)
          + LongEncoder.encodeLength(this.snapshotIndex)
          + LongEncoder.encodeLength(this.pairIndex)
          + (this.pairIndex == 0 ? InstallSnapshot.calculateSize(this.snapshotConfig) : 0)
          + 1
          + Message.calculateSize(this.data);
    }

    private static Map<String, String> getSnapshotConfig(ByteBuffer buf) {
        final int count = (int)LongEncoder.read(buf);
        final HashMap<String, String> config = new HashMap<>(count);
        for (int i = 0; i < count; i++)
            config.put(Message.getString(buf), Message.getString(buf));
        return config;
    }

    private static void putSnapshotConfig(ByteBuffer dest, Map<String, String> config) {
        final int count = config.size();
        LongEncoder.write(dest, count);
        for (Map.Entry<String, String> entry : config.entrySet()) {
            Message.putString(dest, entry.getKey());
            Message.putString(dest, entry.getValue());
        }
    }

    private static int calculateSize(Map<String, String> config) {
        int total = LongEncoder.encodeLength(config.size());
        for (Map.Entry<String, String> entry : config.entrySet())
            total += Message.calculateSize(entry.getKey()) + Message.calculateSize(entry.getValue());
        return total;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",clusterId=" + String.format("%08x", this.getClusterId())
          + ",term=" + this.getTerm()
          + ",snapshotTerm=" + this.snapshotTerm
          + ",snapshotIndex=" + this.snapshotIndex
          + ",pairIndex=" + this.pairIndex
          + (this.snapshotConfig != null ? ",snapshotConfig=" + this.snapshotConfig : "")
          + ",lastChunk=" + this.lastChunk
          + ",data=" + this.describe(this.data)
          + "]";
    }
}

