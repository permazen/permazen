
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import java.nio.ByteBuffer;

/**
 * Response to a {@link PingRequest}.
 */
public class PingResponse extends AbstractPingMessage {

// Constructors

    /**
     * Constructor.
     *
     * @param requestId request ID from the {@link PingRequest}
     */
    public PingResponse(int clusterId, String senderId, String recipientId, long requestId) {
        super(Message.PING_RESPONSE_TYPE, clusterId, senderId, recipientId, requestId);
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

