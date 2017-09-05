
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.msg;

import io.permazen.kv.raft.Timestamp;

import java.nio.ByteBuffer;

/**
 * Sent from hermits to other nodes when trying to establish communication with a majority.
 */
public class PingRequest extends AbstractPingMessage {

// Constructors

    /**
     * Constructor.
     *
     * @param clusterId cluster ID
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     * @param timestamp request timestamp
     */
    public PingRequest(int clusterId, String senderId, String recipientId, long term, Timestamp timestamp) {
        super(Message.PING_REQUEST_TYPE, clusterId, senderId, recipientId, term, timestamp);
        this.checkArguments();
    }

    PingRequest(ByteBuffer buf, int version) {
        super(Message.PING_REQUEST_TYPE, buf, version);
        this.checkArguments();
    }

// Message

    @Override
    public void visit(MessageSwitch handler) {
        handler.casePingRequest(this);
    }
}

