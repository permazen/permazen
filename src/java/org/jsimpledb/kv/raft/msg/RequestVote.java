
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft.msg;

import java.nio.ByteBuffer;

import org.jsimpledb.util.LongEncoder;

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
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param lastLogTerm term of the sender's last log entry
     * @param lastLogIndex index of the sender's last log entry
     */
    public RequestVote(String senderId, String recipientId, long term, long lastLogTerm, long lastLogIndex) {
        super(Message.REQUEST_VOTE_TYPE, senderId, recipientId, term);
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    RequestVote(ByteBuffer buf) {
        super(Message.REQUEST_VOTE_TYPE, buf);
        this.lastLogTerm = LongEncoder.read(buf);
        this.lastLogIndex = LongEncoder.read(buf);
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
    public void writeTo(ByteBuffer dest) {
        super.writeTo(dest);
        LongEncoder.write(dest, this.lastLogTerm);
        LongEncoder.write(dest, this.lastLogIndex);
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + LongEncoder.encodeLength(this.lastLogTerm)
          + LongEncoder.encodeLength(this.lastLogIndex);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",term=" + this.getTerm()
          + ",lastLogTerm=" + this.lastLogTerm
          + ",lastLogIndex=" + this.lastLogIndex
          + "]";
    }
}

