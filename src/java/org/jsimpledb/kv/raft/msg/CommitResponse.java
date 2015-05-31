
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import org.jsimpledb.kv.raft.Timestamp;
import org.jsimpledb.util.LongEncoder;

/**
 * Response to a {@link CommitRequest}.
 *
 * <p>
 * If the request was successful, the leader returns the term and index of the log entry that, when committed (in the Raft sense),
 * allows the transaction to be committed. This suffices for read-write transactions, because the log entry corresponds
 * directly to the transaction's mutations.
 *
 * <p>
 * In order to support linearizable semantics for read-only transactions, which do not create new log entries, in addition
 * a {@linkplain #getCommitLeaderLeaseTimeout minimum leader lease timeout} value is included; the transaction can then
 * be committed after receiving an {@link AppendRequest} whose {@linkplain AppendRequest#getLeaderLeaseTimeout
 * leader lease timeout} is at least this high. In most cases, such an {@link AppendRequest} will have already been received,
 * so the transaction can be committed with a single round trip.
 */
public class CommitResponse extends Message {

    private final long txId;
    private final long commitTerm;
    private final long commitIndex;
    private final Timestamp commitLeaderLeaseTimeout;               // minimum required value we must see to commit
    private final String errorMessage;

// Constructors

    /**
     * Constructor for success case, when there is no minimum leader lease timeout required for commit
     * (read-write transaction, or read-only transaction occurring within the current leader lease timeout).
     *
     * @param clusterId cluster ID
     * @param senderId sending node identity
     * @param recipientId identity of recipient
     * @param term current term
     * @param txId recipient's original transaction ID
     * @param commitTerm transaction commit term
     * @param commitIndex transaction commit index
     */
    public CommitResponse(int clusterId, String senderId, String recipientId, long term, long txId,
      long commitTerm, long commitIndex) {
        this(clusterId, senderId, recipientId, term, txId, commitTerm, commitIndex, null);
    }

    /**
     * Constructor for success case when a minimum leader lease timeout is required for commit (read-only transaction
     * when leader has not heard from a majority of followers in at least a minimum election timeout.
     *
     * @param clusterId cluster ID
     * @param senderId sending node identity
     * @param recipientId identity of recipient
     * @param term current term
     * @param txId recipient's original transaction ID
     * @param commitTerm transaction commit term
     * @param commitIndex transaction commit index
     * @param commitLeaderLeaseTimeout minimum leader lease time required for commit, or null for none
     */
    public CommitResponse(int clusterId, String senderId, String recipientId, long term,
      long txId, long commitTerm, long commitIndex, Timestamp commitLeaderLeaseTimeout) {
        this(clusterId, senderId, recipientId, term, txId, commitTerm, commitIndex, commitLeaderLeaseTimeout, null);
    }

    /**
     * Constructor for error case.
     *
     * @param clusterId cluster ID
     * @param senderId sending node identity
     * @param recipientId identity of recipient
     * @param term current term
     * @param txId recipient's original transaction ID
     * @param errorMessage failure error message
     */
    public CommitResponse(int clusterId, String senderId, String recipientId, long term, long txId, String errorMessage) {
        this(clusterId, senderId, recipientId, term, txId, 0, 0, null, errorMessage);
        Preconditions.checkArgument(errorMessage != null, "null errorMessage");
    }

    private CommitResponse(int clusterId, String senderId, String recipientId, long term, long txId,
      long commitTerm, long commitIndex, Timestamp commitLeaderLeaseTimeout, String errorMessage) {
        super(Message.COMMIT_RESPONSE_TYPE, clusterId, senderId, recipientId, term);
        this.txId = txId;
        this.commitTerm = commitTerm;
        this.commitIndex = commitIndex;
        this.commitLeaderLeaseTimeout = commitLeaderLeaseTimeout;
        this.errorMessage = errorMessage;
        this.checkArguments();
    }

    CommitResponse(ByteBuffer buf) {
        super(Message.COMMIT_RESPONSE_TYPE, buf);
        this.txId = LongEncoder.read(buf);
        this.commitTerm = LongEncoder.read(buf);
        this.commitIndex = LongEncoder.read(buf);
        this.commitLeaderLeaseTimeout = Message.getBoolean(buf) ? Message.getTimestamp(buf) : null;
        this.errorMessage = Message.getBoolean(buf) ? Message.getString(buf) : null;
        this.checkArguments();
    }

    @Override
    void checkArguments() {
        super.checkArguments();
        Preconditions.checkArgument(this.txId != 0);
        Preconditions.checkArgument(this.commitTerm >= 0);
        Preconditions.checkArgument(this.commitIndex >= 0);
        Preconditions.checkArgument(this.errorMessage == null || this.commitLeaderLeaseTimeout == null);
    }

// Properties

    /**
     * Get the sender's ID for the transaction.
     *
     * @return transaction ID
     */
    public long getTxId() {
        return this.txId;
    }

    /**
     * Determine whether the request was successful.
     *
     * @return true for success, false for error
     */
    public boolean isSuccess() {
        return this.errorMessage == null;
    }

    /**
     * Get the commit term for the transaction. This is always the same as the term of the sender.
     *
     * @return transaction commit term
     */
    public long getCommitTerm() {
        return this.commitTerm;
    }

    /**
     * Get the commit index for the transaction.
     *
     * @return transaction commit index, or zero if there was an error
     */
    public long getCommitIndex() {
        return this.commitIndex;
    }

    /**
     * Get the minimum required leader lease timeout value to commit, if any.
     *
     * @return minimum leader lease timeout, or null if there is none
     */
    public Timestamp getCommitLeaderLeaseTimeout() {
        return this.commitLeaderLeaseTimeout;
    }

    /**
     * Get the error message in case of failure.
     *
     * @return failure message, or null if there was no error
     */
    public String getErrorMessage() {
        return this.errorMessage;
    }

// Message

    @Override
    public boolean isLeaderMessage() {
        return true;
    }

    @Override
    public void visit(MessageSwitch handler) {
        handler.caseCommitResponse(this);
    }

    @Override
    public void writeTo(ByteBuffer dest) {
        super.writeTo(dest);
        LongEncoder.write(dest, this.txId);
        LongEncoder.write(dest, this.commitTerm);
        LongEncoder.write(dest, this.commitIndex);
        Message.putBoolean(dest, this.commitLeaderLeaseTimeout != null);
        if (this.commitLeaderLeaseTimeout != null)
            Message.putTimestamp(dest, this.commitLeaderLeaseTimeout);
        Message.putBoolean(dest, this.errorMessage != null);
        if (this.errorMessage != null)
            Message.putString(dest, this.errorMessage);
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + LongEncoder.encodeLength(this.txId)
          + LongEncoder.encodeLength(this.commitTerm)
          + LongEncoder.encodeLength(this.commitIndex)
          + 1
          + (this.commitLeaderLeaseTimeout != null ? Message.calculateSize(this.commitLeaderLeaseTimeout) : 0)
          + 1
          + (this.errorMessage != null ? Message.calculateSize(this.errorMessage) : 0);
    }

// Object

    @Override
    public String toString() {
        final boolean success = this.errorMessage == null;
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",clusterId=" + String.format("%08x", this.getClusterId())
          + ",term=" + this.getTerm()
          + ",txId=" + this.txId
          + (success ?
            ",commit=" + this.commitIndex + "t" + this.commitTerm
             + (this.commitLeaderLeaseTimeout != null ? "@" + this.commitLeaderLeaseTimeout : "") :
            ",error=\"" + this.errorMessage + "\"")
          + "]";
    }
}

