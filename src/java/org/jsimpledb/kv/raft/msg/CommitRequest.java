
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft.msg;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import org.jsimpledb.util.LongEncoder;

/**
 * Sent from followers to leaders to start the commit of a transaction.
 */
public class CommitRequest extends Message {

    private final long txId;
    private final long baseTerm;
    private final long baseIndex;
    private final ByteBuffer readsData;
    private final ByteBuffer writesData;

// Constructors

    /**
     * Constructor.
     *
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param txId sender's transaction ID
     * @param baseTerm term of the log entry on which the transaction is based
     * @param baseIndex index of the log entry on which the transaction is based
     * @param readsData keys read during the transaction
     * @param writesData transaction mutations
     */
    public CommitRequest(String senderId, String recipientId, long term,
      long txId, long baseTerm, long baseIndex, ByteBuffer readsData, ByteBuffer writesData) {
        super(Message.COMMIT_REQUEST_TYPE, senderId, recipientId, term);
        Preconditions.checkArgument(readsData != null, "null readsData");
        this.txId = txId;
        this.baseTerm = baseTerm;
        this.baseIndex = baseIndex;
        this.readsData = readsData;
        this.writesData = writesData;
    }

    CommitRequest(ByteBuffer buf) {
        super(Message.COMMIT_REQUEST_TYPE, buf);
        this.txId = LongEncoder.read(buf);
        this.baseTerm = LongEncoder.read(buf);
        this.baseIndex = LongEncoder.read(buf);
        this.readsData = Message.getByteBuffer(buf);
        this.writesData = Message.getBoolean(buf) ? Message.getByteBuffer(buf) : null;
    }

// Properties

    public long getTxId() {
        return this.txId;
    }

    public long getBaseTerm() {
        return this.baseTerm;
    }

    public long getBaseIndex() {
        return this.baseIndex;
    }

    public ByteBuffer getReadsData() {
        return this.readsData.asReadOnlyBuffer();
    }

    /**
     * Determine whether this is a read-only transaction.
     *
     * @return true if there is no writes data, otherwise false
     */
    public boolean isReadOnly() {
        return this.writesData == null;
    }

    /**
     * Get the transaction's mutations.
     *
     * @return transaction mutations, or null if transaction is read-only
     */
    public ByteBuffer getWritesData() {
        return this.writesData != null ? this.writesData.asReadOnlyBuffer() : null;
    }

// Message

    @Override
    public void visit(MessageSwitch handler) {
        handler.caseCommitRequest(this);
    }

    @Override
    public void writeTo(ByteBuffer dest) {
        super.writeTo(dest);
        LongEncoder.write(dest, this.txId);
        LongEncoder.write(dest, this.baseTerm);
        LongEncoder.write(dest, this.baseIndex);
        Message.putByteBuffer(dest, this.readsData);
        Message.putBoolean(dest, this.writesData != null);
        if (this.writesData != null)
            Message.putByteBuffer(dest, this.writesData);
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + LongEncoder.encodeLength(this.txId)
          + LongEncoder.encodeLength(this.baseTerm)
          + LongEncoder.encodeLength(this.baseIndex)
          + Message.calculateSize(this.readsData)
          + 1
          + (this.writesData != null ? Message.calculateSize(this.writesData) : 0);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",term=" + this.getTerm()
          + ",txId=" + this.txId
          + ",base=" + this.baseTerm + "/" + this.baseIndex
          + ",readsData=" + this.describe(this.readsData)
          + (this.writesData != null ? ",writesData=" + this.describe(this.writesData) : "")
          + "]";
    }
}

