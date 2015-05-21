
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft.msg;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import org.jsimpledb.kv.raft.Timestamp;
import org.jsimpledb.util.LongEncoder;

/**
 * Sent from leaders to followers to probe the follower's log state and/or append an entry to their log.
 *
 * <p>
 * Instances also provide the {@linkplain #getLeaderLeaseTimeout leader's lease timeout} value, which is
 * used to commit read-only transactions, as well as a {@linkplain #getLeaderTimestamp leader timestamp}
 * which should be reflected back in the corresponding {@link AppendResponse}.
 */
public class AppendRequest extends Message {

    private final Timestamp leaderTimestamp;        // leader's timestamp for this request
    private final Timestamp leaderLeaseTimeout;     // earliest leader timestamp at which time leader could be deposed
    private final long leaderCommit;                // index of highest log entry known to be committed
    private final long prevLogTerm;                 // term of previous log entry
    private final long prevLogIndex;                // index of previous log entry
    private final long logEntryTerm;                // term corresponding to log entry, or zero if this is a "probe"
    private final ByteBuffer writesData;            // mutation data, if not a probe and not from follower transaction

// Constructors

    /**
     * Construtor for a "probe" that does not contain a log entry.
     *
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param leaderTimestamp leader's timestamp for this request
     * @param leaderLeaseTimeout earliest leader timestamp at which leader could be deposed
     * @param leaderCommit current commit index for sender
     * @param prevLogTerm term of the log entry just prior to this one
     * @param prevLogIndex index of the log entry just prior to this one
     */
    public AppendRequest(String senderId, String recipientId, long term,
      Timestamp leaderTimestamp, Timestamp leaderLeaseTimeout, long leaderCommit, long prevLogTerm, long prevLogIndex) {
        this(senderId, recipientId, term, leaderTimestamp, leaderLeaseTimeout, leaderCommit, prevLogTerm, prevLogIndex, 0, null);
    }

    /**
     * Constructor for a request that contains an actual log entry.
     *
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param leaderTimestamp leader's timestamp for this request
     * @param leaderLeaseTimeout earliest leader timestamp at which leader could be deposed
     * @param leaderCommit current commit index for sender
     * @param prevLogTerm term of the log entry just prior to this one
     * @param prevLogIndex index of the log entry just prior to this one
     * @param logEntryTerm term of this log entry
     * @param writesData log entry serialized mutations, or null if follower should have the data already
     */
    public AppendRequest(String senderId, String recipientId, long term, Timestamp leaderTimestamp, Timestamp leaderLeaseTimeout,
      long leaderCommit, long prevLogTerm, long prevLogIndex, long logEntryTerm, ByteBuffer writesData) {
        super(Message.APPEND_REQUEST_TYPE, senderId, recipientId, term);
        Preconditions.checkArgument(leaderTimestamp != null, "null leaderTimestamp");
        Preconditions.checkArgument(leaderLeaseTimeout != null, "null leaderLeaseTimeout");
        Preconditions.checkArgument(writesData == null || logEntryTerm > 0);
        this.leaderTimestamp = leaderTimestamp;
        this.leaderLeaseTimeout = leaderLeaseTimeout;
        this.leaderCommit = leaderCommit;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.logEntryTerm = logEntryTerm;
        this.writesData = writesData;
    }

    AppendRequest(ByteBuffer buf) {
        super(Message.APPEND_REQUEST_TYPE, buf);
        this.leaderTimestamp = Message.getTimestamp(buf);
        this.leaderLeaseTimeout = new Timestamp(this.leaderTimestamp.getMillis() + (int)LongEncoder.read(buf));
        this.leaderCommit = LongEncoder.read(buf);
        this.prevLogTerm = LongEncoder.read(buf);
        this.prevLogIndex = LongEncoder.read(buf);
        this.logEntryTerm = LongEncoder.read(buf);
        this.writesData = this.logEntryTerm != 0 && Message.getBoolean(buf) ? Message.getByteBuffer(buf) : null;
    }

// Properties

    public Timestamp getLeaderTimestamp() {
        return this.leaderTimestamp;
    }

    public Timestamp getLeaderLeaseTimeout() {
        return this.leaderLeaseTimeout;
    }

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
     * Get the serialized {@link org.jsimpledb.kv.mvcc.Writes} for the log entry, if any.
     * Returns null if this is a probe or follower is expected to already have the data from a transaction.
     *
     * @return log entry {@link org.jsimpledb.kv.mvcc.Writes}, or null if this message does not contain the data
     */
    public ByteBuffer getWritesData() {
        return this.writesData != null ? this.writesData.asReadOnlyBuffer() : null;
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
    public void writeTo(ByteBuffer dest) {
        super.writeTo(dest);
        Message.putTimestamp(dest, this.leaderTimestamp);
        LongEncoder.write(dest, this.leaderLeaseTimeout.getMillis() - this.leaderTimestamp.getMillis());
        LongEncoder.write(dest, this.leaderCommit);
        LongEncoder.write(dest, this.prevLogTerm);
        LongEncoder.write(dest, this.prevLogIndex);
        LongEncoder.write(dest, this.logEntryTerm);
        if (this.logEntryTerm != 0) {
            Message.putBoolean(dest, this.writesData != null);
            if (this.writesData != null)
                Message.putByteBuffer(dest, this.writesData);
        }
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + Message.calculateSize(this.leaderTimestamp)
          + LongEncoder.encodeLength(this.leaderLeaseTimeout.getMillis() - this.leaderTimestamp.getMillis())
          + LongEncoder.encodeLength(this.leaderCommit)
          + LongEncoder.encodeLength(this.prevLogTerm)
          + LongEncoder.encodeLength(this.prevLogIndex)
          + LongEncoder.encodeLength(this.logEntryTerm)
          + (this.logEntryTerm != 0 ? 1 + (this.writesData != null ? Message.calculateSize(this.writesData) : 0) : 0);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",term=" + this.getTerm()
          + ",leaderTimestamp=" + this.leaderTimestamp
          + ",leaderLeaseTimeout=" + this.leaderLeaseTimeout
          + ",leaderCommit=" + this.leaderCommit
          + ",prevLog=" + this.prevLogTerm + "/" + this.prevLogIndex
          + (this.logEntryTerm != 0 ? ",logEntryTerm=" + this.logEntryTerm : "")
          + (this.writesData != null ? ",writesData=" + this.describe(this.writesData) : "")
          + "]";
    }
}

