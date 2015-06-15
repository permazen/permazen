
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import java.nio.ByteBuffer;

/**
 * Sent from hermits to other nodes when trying to establish communication with a majority.
 */
public class PingRequest extends AbstractPingMessage {

// Constructors

    /**
     * Constructor.
     *
     * @param requestId request ID
     */
    public PingRequest(int clusterId, String senderId, String recipientId, long requestId) {
        super(Message.PING_REQUEST_TYPE, clusterId, senderId, recipientId, requestId);
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

