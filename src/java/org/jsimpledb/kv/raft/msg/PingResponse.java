
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import java.nio.ByteBuffer;

import org.jsimpledb.kv.raft.Timestamp;

/**
 * Response to a {@link PingRequest}.
 */
public class PingResponse extends AbstractPingMessage {

// Constructors

    /**
     * Constructor.
     *
     * @param timestamp request timestamp from the {@link PingRequest}
     */
    public PingResponse(int clusterId, String senderId, String recipientId, long term, Timestamp timestamp) {
        super(Message.PING_RESPONSE_TYPE, clusterId, senderId, recipientId, term, timestamp);
        this.checkArguments();
    }

    PingResponse(ByteBuffer buf) {
        super(Message.PING_RESPONSE_TYPE, buf);
        this.checkArguments();
    }

// Message

    @Override
    public void visit(MessageSwitch handler) {
        handler.casePingResponse(this);
    }
}

