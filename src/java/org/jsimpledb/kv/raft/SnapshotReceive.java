
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dellroad.stuff.io.ByteBufferInputStream;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.raft.msg.InstallSnapshot;
import org.jsimpledb.kv.util.KeyListEncoder;

/**
 * Represents and in-progress snapshot installation from the follower's point of view.
 *
 * <p>
 * Instances are not thread safe.
 */
class SnapshotReceive {

    private final KVStore kv;
    private final long snapshotTerm;
    private final long snapshotIndex;

    private long pairIndex;
    private byte[] previousKey;

// Constructors

    public SnapshotReceive(KVStore kv, long snapshotTerm, long snapshotIndex) {
        Preconditions.checkArgument(kv != null, "null kv");
        this.snapshotTerm = snapshotTerm;
        this.snapshotIndex = snapshotIndex;
        this.kv = kv;
    }

// Public methods

    public long getSnapshotTerm() {
        return this.snapshotTerm;
    }

    public long getSnapshotIndex() {
        return this.snapshotIndex;
    }

    public long getPairIndex() {
        return this.pairIndex;
    }

    /**
     * Apply the next chunk of key/value pairs.
     *
     * @param buf encoded key/value pairs
     * @throws IllegalArgumentException if {@code buf} contains invalid data
     * @throws IllegalArgumentException if {@code buf} is null
     */
    public void applyNextChunk(ByteBuffer buf) {

        // Sanity check
        Preconditions.checkArgument(buf != null, "null buf");

        // Read and apply key/value pairs
        final ByteBufferInputStream input = new ByteBufferInputStream(buf);
        while (buf.hasRemaining()) {

            // Decode next key/value pair
            final byte[] key;
            final byte[] value;
            try {
                key = KeyListEncoder.read(input, this.previousKey);
                value = KeyListEncoder.read(input, null);
            } catch (IOException e) {
                throw new IllegalArgumentException("invalid encoded key/value data", e.getCause());
            }
            this.previousKey = key;
            this.pairIndex++;

            // Apply key/value pair
            this.kv.put(key, value);
        }
    }

    public boolean matches(InstallSnapshot msg) {
        return this.snapshotTerm == msg.getSnapshotTerm()
          && this.snapshotIndex == msg.getSnapshotIndex()
          && this.pairIndex == msg.getPairIndex();
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[snapshotTerm=" + this.snapshotTerm
          + ",snapshotIndex=" + this.snapshotIndex
          + ",pairIndex=" + this.pairIndex
          + "]";
    }
}

