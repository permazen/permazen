
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.msg;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import io.permazen.util.LongEncoder;

/**
 * Send from candidates to other nodes to request a vote during an election.
 */
public class RequestVote extends Message {

    private final long lastLogTerm;
    private final long lastLogIndex;

// Constructors

    /**
     * Constructor.
     *
     * @param clusterId cluster ID
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param lastLogTerm term of the sender's last log entry
     * @param lastLogIndex index of the sender's last log entry
     */
    public RequestVote(int clusterId, String senderId, String recipientId, long term, long lastLogTerm, long lastLogIndex) {
        super(Message.REQUEST_VOTE_TYPE, clusterId, senderId, recipientId, term);
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
        this.checkArguments();
    }

    RequestVote(ByteBuffer buf, int version) {
        super(Message.REQUEST_VOTE_TYPE, buf, version);
        this.lastLogTerm = LongEncoder.read(buf);
        this.lastLogIndex = LongEncoder.read(buf);
        this.checkArguments();
    }

    @Override
    void checkArguments() {
        super.checkArguments();
        Preconditions.checkArgument(this.lastLogTerm > 0);
        Preconditions.checkArgument(this.lastLogIndex > 0);
    }

// Properties

    public long getLastLogTerm() {
        return this.lastLogTerm;
    }

    public long getLastLogIndex() {
        return this.lastLogIndex;
    }

// Message

    @Override
    public void visit(MessageSwitch handler) {
        handler.caseRequestVote(this);
    }

    @Override
    public void writeTo(ByteBuffer dest, int version) {
        super.writeTo(dest, version);
        LongEncoder.write(dest, this.lastLogTerm);
        LongEncoder.write(dest, this.lastLogIndex);
    }

    @Override
    protected int calculateSize(int version) {
        return super.calculateSize(version)
          + LongEncoder.encodeLength(this.lastLogTerm)
          + LongEncoder.encodeLength(this.lastLogIndex);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",clusterId=" + String.format("%08x", this.getClusterId())
          + ",term=" + this.getTerm()
          + ",lastLogTerm=" + this.lastLogTerm
          + ",lastLogIndex=" + this.lastLogIndex
          + "]";
    }
}

