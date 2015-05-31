
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
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

    private ByteBuffer mutationData;
    private boolean mutationDataInvalid;                        // mutationData has already been grabbed

// Constructors

    /**
     * Constructor.
     *
     * @param clusterId cluster ID
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param txId sender's transaction ID
     * @param baseTerm term of the log entry on which the transaction is based
     * @param baseIndex index of the log entry on which the transaction is based
     * @param readsData keys read during the transaction
     * @param mutationData transaction mutations, or null for none
     */
    public CommitRequest(int clusterId, String senderId, String recipientId, long term,
      long txId, long baseTerm, long baseIndex, ByteBuffer readsData, ByteBuffer mutationData) {
        super(Message.COMMIT_REQUEST_TYPE, clusterId, senderId, recipientId, term);
        this.txId = txId;
        this.baseTerm = baseTerm;
        this.baseIndex = baseIndex;
        this.readsData = readsData;
        this.mutationData = mutationData;
        this.checkArguments();
    }

    CommitRequest(ByteBuffer buf) {
        super(Message.COMMIT_REQUEST_TYPE, buf);
        this.txId = LongEncoder.read(buf);
        this.baseTerm = LongEncoder.read(buf);
        this.baseIndex = LongEncoder.read(buf);
        this.readsData = Message.getByteBuffer(buf);
        this.mutationData = Message.getBoolean(buf) ? Message.getByteBuffer(buf) : null;
        this.checkArguments();
    }

    @Override
    void checkArguments() {
        super.checkArguments();
        Preconditions.checkArgument(this.txId != 0);
        Preconditions.checkArgument(this.baseTerm >= 0);
        Preconditions.checkArgument(this.baseIndex >= 0);
        Preconditions.checkArgument(this.readsData != null);
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
        return this.mutationData == null;
    }

    /**
     * Get the transaction's mutations.
     *
     * <p>
     * This method may only be invoked once.
     *
     * @return transaction mutations, or null if transaction is read-only
     * @throws IllegalStateException if this method has already been invoked
     */
    public ByteBuffer getMutationData() {
        Preconditions.checkState(!this.mutationDataInvalid);
        final ByteBuffer result = this.mutationData;
        this.mutationData = null;
        this.mutationDataInvalid = true;
        return result;
    }

// Message

    @Override
    public void visit(MessageSwitch handler) {
        handler.caseCommitRequest(this);
    }

    @Override
    public void writeTo(ByteBuffer dest) {
        Preconditions.checkState(!this.mutationDataInvalid);
        super.writeTo(dest);
        LongEncoder.write(dest, this.txId);
        LongEncoder.write(dest, this.baseTerm);
        LongEncoder.write(dest, this.baseIndex);
        Message.putByteBuffer(dest, this.readsData);
        Message.putBoolean(dest, this.mutationData != null);
        if (this.mutationData != null)
            Message.putByteBuffer(dest, this.mutationData);
    }

    @Override
    protected int calculateSize() {
        Preconditions.checkState(!this.mutationDataInvalid);
        return super.calculateSize()
          + LongEncoder.encodeLength(this.txId)
          + LongEncoder.encodeLength(this.baseTerm)
          + LongEncoder.encodeLength(this.baseIndex)
          + Message.calculateSize(this.readsData)
          + 1
          + (this.mutationData != null ? Message.calculateSize(this.mutationData) : 0);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",clusterId=" + String.format("%08x", this.getClusterId())
          + ",term=" + this.getTerm()
          + ",txId=" + this.txId
          + ",base=" + this.baseIndex + "t" + this.baseTerm
          + ",readsData=" + this.describe(this.readsData)
          + (this.mutationData != null ?
            ",mutationData=" + this.describe(this.mutationData) : this.mutationDataInvalid ? ",mutationData=invalid" : "")
          + "]";
    }
}

