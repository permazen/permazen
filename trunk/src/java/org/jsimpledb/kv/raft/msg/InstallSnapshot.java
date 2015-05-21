
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft.msg;

import java.nio.ByteBuffer;

import org.jsimpledb.util.LongEncoder;

/**
 * Sent from leader to follower to with a chunk of key/value pairs that will wholesale replace the follower's key/value store.
 */
public class InstallSnapshot extends Message {

    private final long snapshotTerm;
    private final long snapshotIndex;
    private final long pairIndex;
    private final boolean lastChunk;
    private final ByteBuffer data;

// Constructors

    /**
     * Constructor.
     *
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param snapshotTerm term of the last log entry in the snapshot
     * @param snapshotIndex index of the last log entry in the snapshot
     * @param pairIndex index of the first key/value pair in this chunk
     * @param lastChunk true if this is the last chunk in the snapshot
     * @param data encoded key/value pairs
     */
    public InstallSnapshot(String senderId, String recipientId, long term,
      long snapshotTerm, long snapshotIndex, long pairIndex, boolean lastChunk, ByteBuffer data) {
        super(Message.INSTALL_SNAPSHOT_TYPE, senderId, recipientId, term);
        this.snapshotTerm = snapshotTerm;
        this.snapshotIndex = snapshotIndex;
        this.pairIndex = pairIndex;
        this.lastChunk = lastChunk;
        this.data = data;
    }

    InstallSnapshot(ByteBuffer buf) {
        super(Message.INSTALL_SNAPSHOT_TYPE, buf);
        this.snapshotTerm = LongEncoder.read(buf);
        this.snapshotIndex = LongEncoder.read(buf);
        this.pairIndex = LongEncoder.read(buf);
        this.lastChunk = Message.getBoolean(buf);
        this.data = Message.getByteBuffer(buf);
    }

// Properties

    public long getSnapshotTerm() {
        return this.snapshotTerm;
    }

    public long getSnapshotIndex() {
        return this.snapshotIndex;
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
        Message.putBoolean(dest, this.lastChunk);
        Message.putByteBuffer(dest, this.data);
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + LongEncoder.encodeLength(this.snapshotTerm)
          + LongEncoder.encodeLength(this.snapshotIndex)
          + LongEncoder.encodeLength(this.pairIndex)
          + 1
          + Message.calculateSize(this.data);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",term=" + this.getTerm()
          + ",snapshotTerm=" + this.snapshotTerm
          + ",snapshotIndex=" + this.snapshotIndex
          + ",pairIndex=" + this.pairIndex
          + ",lastChunk=" + this.lastChunk
          + ",data=" + this.describe(this.data)
          + "]";
    }
}

