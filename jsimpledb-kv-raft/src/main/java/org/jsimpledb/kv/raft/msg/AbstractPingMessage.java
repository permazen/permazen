
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import org.jsimpledb.kv.raft.Timestamp;

abstract class AbstractPingMessage extends Message {

    private final Timestamp timestamp;

// Constructors

    /**
     * Constructor.
     *
     * @param timestamp request timestamp
     */
    AbstractPingMessage(byte type, int clusterId, String senderId, String recipientId, long term, Timestamp timestamp) {
        super(type, clusterId, senderId, recipientId, term);
        this.timestamp = timestamp;
    }

    AbstractPingMessage(byte type, ByteBuffer buf) {
        super(type, buf);
        this.timestamp = Message.getTimestamp(buf);
    }

    @Override
    void checkArguments() {
        super.checkArguments();
        Preconditions.checkArgument(this.timestamp != null);
    }

// Properties

    /**
     * Get the ping request timestamp.
     *
     * @return ping request timestamp
     */
    public Timestamp getTimestamp() {
        return this.timestamp;
    }

// Message

    @Override
    public void writeTo(ByteBuffer dest) {
        super.writeTo(dest);
        Message.putTimestamp(dest, this.timestamp);
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + Message.calculateSize(this.timestamp);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[\"" + this.getSenderId() + "\"->\"" + this.getRecipientId() + "\""
          + ",clusterId=" + String.format("%08x", this.getClusterId())
          + ",term=" + this.getTerm()
          + ",timestamp=" + this.timestamp
          + "]";
    }
}

