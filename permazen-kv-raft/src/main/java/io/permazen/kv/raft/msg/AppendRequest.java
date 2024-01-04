
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.msg;

import com.google.common.base.Preconditions;

import io.permazen.kv.raft.Timestamp;
import io.permazen.util.LongEncoder;

import java.nio.ByteBuffer;

/**
 * Sent from leaders to followers to probe the follower's log state and/or append an entry to their log.
 *
 * <p>
 * Instances also provide the {@linkplain #getLeaderLeaseTimeout leader's lease timeout} value (if any),
 * which is used to commit read-only transactions, as well as a {@linkplain #getLeaderTimestamp leader timestamp}
 * which should be reflected back in the corresponding {@link AppendResponse}.
 */
public class AppendRequest extends Message {

    private final Timestamp leaderTimestamp;        // leader's timestamp for this request
    private final Timestamp leaderLeaseTimeout;     // earliest leader timestamp at which time leader could be deposed
    private final long leaderCommit;                // index of highest log entry known to be committed
    private final long prevLogTerm;                 // term of previous log entry
    private final long prevLogIndex;                // index of previous log entry
    private final long logEntryTerm;                // term corresponding to log entry, or zero if this is a "probe"

    private ByteBuffer mutationData;                // serialized mutations, if not a probe and not from follower transaction
    private boolean mutationDataInvalid;            // mutationData has already been grabbed

// Constructors

    /**
     * Constructor for a "probe" that does not contain a log entry.
     *
     * <p>
     * Note that {@code leaderCommit} is limited to {@code prevLogIndex} by this constructor, because we can't guarantee
     * that the omitted log entry at {@code prevLogIndex + 1} matches whatever the follower has in its log.
     *
     * @param clusterId cluster ID
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param leaderTimestamp leader's timestamp for this request
     * @param leaderLeaseTimeout earliest leader timestamp at which leader could be deposed (or null)
     * @param leaderCommit current commit index for sender
     * @param prevLogTerm term of the log entry just prior to this one
     * @param prevLogIndex index of the log entry just prior to this one
     */
    public AppendRequest(int clusterId, String senderId, String recipientId, long term,
      Timestamp leaderTimestamp, Timestamp leaderLeaseTimeout, long leaderCommit, long prevLogTerm, long prevLogIndex) {
        this(clusterId, senderId, recipientId, term, leaderTimestamp, leaderLeaseTimeout, Math.min(leaderCommit, prevLogIndex),
          prevLogTerm, prevLogIndex, 0, null);
    }

    /**
     * Constructor for a request that contains an actual log entry.
     *
     * @param clusterId cluster ID
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param leaderTimestamp leader's timestamp for this request
     * @param leaderLeaseTimeout earliest leader timestamp at which leader could be deposed (or null)
     * @param leaderCommit current commit index for sender
     * @param prevLogTerm term of the log entry just prior to this one
     * @param prevLogIndex index of the log entry just prior to this one
     * @param logEntryTerm term of this log entry
     * @param mutationData log entry serialized mutations, or null if follower should have the data already
     */
    @SuppressWarnings("this-escape")
    public AppendRequest(int clusterId, String senderId, String recipientId, long term, Timestamp leaderTimestamp,
      Timestamp leaderLeaseTimeout, long leaderCommit, long prevLogTerm, long prevLogIndex, long logEntryTerm,
      ByteBuffer mutationData) {
        super(Message.APPEND_REQUEST_TYPE, clusterId, senderId, recipientId, term);
        this.leaderTimestamp = leaderTimestamp;
        this.leaderLeaseTimeout = leaderLeaseTimeout;
        this.leaderCommit = leaderCommit;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.logEntryTerm = logEntryTerm;
        this.mutationData = mutationData;
        this.checkArguments();
    }

    AppendRequest(ByteBuffer buf, int version) {
        super(Message.APPEND_REQUEST_TYPE, buf, version);
        this.leaderTimestamp = Message.getTimestamp(buf, version);
        this.leaderLeaseTimeout = Message.getBoolean(buf) ? this.leaderTimestamp.offset((int)LongEncoder.read(buf)) : null;
        this.leaderCommit = LongEncoder.read(buf);
        this.prevLogTerm = LongEncoder.read(buf);
        this.prevLogIndex = LongEncoder.read(buf);
        this.logEntryTerm = LongEncoder.read(buf);
        this.mutationData = this.logEntryTerm != 0 && Message.getBoolean(buf) ? Message.getByteBuffer(buf) : null;
        this.checkArguments();
    }

    @Override
    void checkArguments() {
        super.checkArguments();
        Preconditions.checkArgument(this.leaderTimestamp != null);
        Preconditions.checkArgument(this.leaderCommit >= 0);
        Preconditions.checkArgument(this.prevLogTerm >= 0);
        Preconditions.checkArgument(this.prevLogIndex >= 0);
        Preconditions.checkArgument(this.logEntryTerm >= 0);
        Preconditions.checkArgument(this.mutationData == null || this.logEntryTerm > 0);
    }

// Properties

    public Timestamp getLeaderTimestamp() {
        return this.leaderTimestamp;
    }

    public Timestamp getLeaderLeaseTimeout() {
        return this.leaderLeaseTimeout;
    }

    /**
     * Get leader's commit index.
     *
     * <p>
     * For {@linkplain #isProbe probe} requests, this value is limited to the value returned by {@link #getPrevLogIndex}.
     *
     * @return leader's commit index
     */
    public long getLeaderCommit() {
        return this.leaderCommit;
    }

    public long getPrevLogTerm() {
        return this.prevLogTerm;
    }

    public long getPrevLogIndex() {
        return this.prevLogIndex;
    }

    public boolean isProbe() {
        return this.logEntryTerm == 0;
    }

    public long getLogEntryTerm() {
        return this.logEntryTerm;
    }

    /**
     * Get the serialized data for the log entry, if any.
     * Returns null if this is a probe or follower is expected to already have the data from a transaction.
     *
     * <p>
     * This method may only be invoked once.
     *
     * @return log entry serialized mutations, or null if this message does not contain data
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
    public boolean isLeaderMessage() {
        return true;
    }

    @Override
    public void visit(MessageSwitch handler) {
        handler.caseAppendRequest(this);
    }

    @Override
    public void writeTo(ByteBuffer dest, int version) {
        Preconditions.checkState(!this.mutationDataInvalid);
        super.writeTo(dest, version);
        Message.putTimestamp(dest, this.leaderTimestamp, version);
        Message.putBoolean(dest, this.leaderLeaseTimeout != null);
        if (this.leaderLeaseTimeout != null)
            LongEncoder.write(dest, this.leaderLeaseTimeout.offsetFrom(this.leaderTimestamp));
        LongEncoder.write(dest, this.leaderCommit);
        LongEncoder.write(dest, this.prevLogTerm);
        LongEncoder.write(dest, this.prevLogIndex);
        LongEncoder.write(dest, this.logEntryTerm);
        if (this.logEntryTerm != 0) {
            Message.putBoolean(dest, this.mutationData != null);
            if (this.mutationData != null)
                Message.putByteBuffer(dest, this.mutationData);
        }
    }

    @Override
    protected int calculateSize(int version) {
        Preconditions.checkState(!this.mutationDataInvalid);
        return super.calculateSize(version)
          + Message.calculateSize(this.leaderTimestamp, version)
          + 1
          + (this.leaderLeaseTimeout != null ?
            LongEncoder.encodeLength(this.leaderLeaseTimeout.offsetFrom(this.leaderTimestamp)) : 0)
          + LongEncoder.encodeLength(this.leaderCommit)
          + LongEncoder.encodeLength(this.prevLogTerm)
          + LongEncoder.encodeLength(this.prevLogIndex)
          + LongEncoder.encodeLength(this.logEntryTerm)
          + (this.logEntryTerm != 0 ? 1 + (this.mutationData != null ? Message.calculateSize(this.mutationData) : 0) : 0);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",clusterId=" + String.format("%08x", this.getClusterId())
          + ",term=" + this.getTerm()
          + ",leaderTimestamp=" + this.leaderTimestamp
          + (this.leaderLeaseTimeout != null ?
            ",leaderLeaseTimeout=" + String.format("%+dms", this.leaderLeaseTimeout.offsetFrom(this.leaderTimestamp)) : "")
          + ",leaderCommit=" + this.leaderCommit
          + ",prevLog=" + this.prevLogIndex + "t" + this.prevLogTerm
          + (this.logEntryTerm != 0 ? ",logEntryTerm=" + this.logEntryTerm : "")
          + (this.mutationData != null ?
            ",mutationData=" + this.describe(this.mutationData) : this.mutationDataInvalid ? ",mutationData=invalid" : "")
          + "]";
    }
}
