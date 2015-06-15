
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import java.nio.ByteBuffer;

import org.jsimpledb.util.LongEncoder;

abstract class AbstractPingMessage extends Message {

    private final long requestId;

// Constructors

    /**
     * Constructor.
     *
     * @param requestId request ID
     */
    AbstractPingMessage(byte type, int clusterId, String senderId, String recipientId, long requestId) {
        super(type, clusterId, senderId, recipientId);
        this.requestId = requestId;
    }

    AbstractPingMessage(byte type, ByteBuffer buf) {
        super(type, buf);
        this.requestId = LongEncoder.read(buf);
    }

// Properties

    /**
     * Get the ping request ID. This is an arbitrary value set by the originator of the ping.
     *
     * @return ping request ID
     */
    public long getRequestId() {
        return this.requestId;
    }

// Message

    @Override
    public void writeTo(ByteBuffer dest) {
        super.writeTo(dest);
        LongEncoder.write(dest, this.requestId);
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + LongEncoder.encodeLength(this.requestId);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",clusterId=" + String.format("%08x", this.getClusterId())
          + ",requestId=" + this.requestId
          + "]";
    }
}

