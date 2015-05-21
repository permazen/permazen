
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft.msg;

import java.nio.ByteBuffer;

/**
 * Sent from a follower to a candidate to grant a vote during an election.
 */
public class GrantVote extends Message {

// Constructors

    /**
     * Constructor.
     *
     * @param senderId sending node identity
     * @param recipientId identity of recipient
     * @param term current term
     */
    public GrantVote(String senderId, String recipientId, long term) {
        super(Message.GRANT_VOTE_TYPE, senderId, recipientId, term);
    }

    GrantVote(ByteBuffer buf) {
        super(Message.GRANT_VOTE_TYPE, buf);
    }

// Message

    @Override
    public void visit(MessageSwitch handler) {
        handler.caseGrantVote(this);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",term=" + this.getTerm()
          + "]";
    }
}

