
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.msg;

import com.google.common.base.Preconditions;

import io.permazen.kv.raft.Timestamp;
import io.permazen.util.LongEncoder;

import java.nio.ByteBuffer;

/**
 * Sent from a follower to a leader in response to an {@link AppendRequest}.
 *
 * <p>
 * Also contains information about the follower's log.
 */
public class AppendResponse extends Message {

    private final Timestamp leaderTimestamp;        // leaderTimestamp from corresponding AppendRequest
    private final boolean success;                  // true if previous log entry term and index matched
    private final long matchIndex;                  // index of highest log entry known to match leader
    private final long lastLogIndex;                // the index of the last log entry in follower's log

// Constructors

    /**
     * Constructor.
     *
     * @param clusterId cluster ID
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param leaderTimestamp copy of {@link AppendRequest#getLeaderTimestamp}
     * @param success if request successfully matched
     * @param matchIndex highest known matching log index
     * @param lastLogIndex index of follower's last log entry
     */
    public AppendResponse(int clusterId, String senderId, String recipientId, long term, Timestamp leaderTimestamp,
      boolean success, long matchIndex, long lastLogIndex) {
        super(Message.APPEND_RESPONSE_TYPE, clusterId, senderId, recipientId, term);
        this.leaderTimestamp = leaderTimestamp;
        this.success = success;
        this.matchIndex = matchIndex;
        this.lastLogIndex = lastLogIndex;
        this.checkArguments();
    }

    AppendResponse(ByteBuffer buf, int version) {
        super(Message.APPEND_RESPONSE_TYPE, buf, version);
        this.leaderTimestamp = Message.getTimestamp(buf, version);
        this.success = Message.getBoolean(buf);
        this.matchIndex = LongEncoder.read(buf);
        this.lastLogIndex = LongEncoder.read(buf);
        this.checkArguments();
    }

    @Override
    void checkArguments() {
        super.checkArguments();
        Preconditions.checkArgument(this.leaderTimestamp != null);
        Preconditions.checkArgument(this.matchIndex >= -1);
        Preconditions.checkArgument(this.matchIndex <= this.lastLogIndex);
    }

// Properties

    /**
     * Get the {@code leaderTimestamp} from the corresponding {@link AppendRequest}.
     *
     * @return request leader timestamp
     * @see AppendRequest#getLeaderTimestamp
     */
    public Timestamp getLeaderTimestamp() {
        return this.leaderTimestamp;
    }

    /**
     * Determine whether the request was successful, i.e., the previous log entry term and index matched.
     *
     * @return true if the corresponding request matched
     */
    public boolean isSuccess() {
        return this.success;
    }

    /**
     * Get the index of the most recent log entry known to match leader.
     * In case of a successful request, this will be equal to the log entry sent (or the previous
     * log entry in case of a probe). In case of a failed request, this will be equal
     * to the follower's state machine last applied index.
     *
     * @return highest known matching log entry index
     */
    public long getMatchIndex() {
        return this.matchIndex;
    }

    /**
     * Get the index of the last log entry in the follower's log.
     *
     * @return last log entry index
     */
    public long getLastLogIndex() {
        return this.lastLogIndex;
    }

// Message

    @Override
    public void visit(MessageSwitch handler) {
        handler.caseAppendResponse(this);
    }

    @Override
    public void writeTo(ByteBuffer dest, int version) {
        super.writeTo(dest, version);
        Message.putTimestamp(dest, this.leaderTimestamp, version);
        Message.putBoolean(dest, this.success);
        LongEncoder.write(dest, this.matchIndex);
        LongEncoder.write(dest, this.lastLogIndex);
    }

    @Override
    protected int calculateSize(int version) {
        return super.calculateSize(version)
          + Message.calculateSize(this.leaderTimestamp, version)
          + 1
          + LongEncoder.encodeLength(this.matchIndex)
          + LongEncoder.encodeLength(this.lastLogIndex);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",clusterId=" + String.format("%08x", this.getClusterId())
          + ",term=" + this.getTerm()
          + ",leaderTimestamp=" + this.leaderTimestamp
          + ",success=" + this.success
          + ",matchIndex=" + this.matchIndex
          + ",lastLogIndex=" + this.lastLogIndex
          + "]";
    }
}

