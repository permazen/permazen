
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.msg;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import org.jsimpledb.util.LongEncoder;

/**
 * Support superclass for Raft messages that contain the sender's current term.
 */
public abstract class AbstractTermedMessage extends Message {

    private final long term;

    /**
     * Constructor.
     *
     * @param clusterId cluster ID
     * @param senderId identity of sender
     * @param recipientId identity of recipient
     * @param term sender's current term
     */
    protected AbstractTermedMessage(byte type, int clusterId, String senderId, String recipientId, long term) {
        super(type, clusterId, senderId, recipientId);
        this.term = term;
    }

    AbstractTermedMessage(byte type, ByteBuffer buf) {
        super(type, buf);
        this.term = LongEncoder.read(buf);
    }

    @Override
    void checkArguments() {
        super.checkArguments();
        Preconditions.checkArgument(this.term > 0);
    }

// Properties

    /**
     * Get the sender's current term.
     *
     * @return sender's current term
     */
    public long getTerm() {
        return this.term;
    }

// Message

    @Override
    public void writeTo(ByteBuffer dest) {
        super.writeTo(dest);
        LongEncoder.write(dest, this.term);
    }

    @Override
    protected int calculateSize() {
        return super.calculateSize()
          + LongEncoder.encodeLength(this.term);
    }
}

