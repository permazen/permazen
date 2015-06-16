
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import java.nio.ByteBuffer;

import org.jsimpledb.kv.raft.Timestamp;

/**
 * Sent from hermits to other nodes when trying to establish communication with a majority.
 */
public class PingRequest extends AbstractPingMessage {

// Constructors

    /**
     * Constructor.
     *
     * @param timestamp request timestamp
     */
    public PingRequest(int clusterId, String senderId, String recipientId, long term, Timestamp timestamp) {
        super(Message.PING_REQUEST_TYPE, clusterId, senderId, recipientId, term, timestamp);
        this.checkArguments();
    }

    PingRequest(ByteBuffer buf) {
        super(Message.PING_REQUEST_TYPE, buf);
        this.checkArguments();
    }

// Message

    @Override
    public void visit(MessageSwitch handler) {
        handler.casePingRequest(this);
    }
}

