
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.dellroad.stuff.io.ByteBufferOutputStream;
import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.util.KeyListEncoder;
import org.jsimpledb.kv.util.PrefixKVStore;

/**
 * Represents and in-progress snapshot installation from the leader's point of view.
 *
 * <p>
 * Instances are not thread safe.
 */
class SnapshotTransmit implements Closeable {

    private static final int MAX_CHUNK_SIZE = 10250;                // about 7 TCP packets

    private final Timestamp createTime = new Timestamp();
    private final long snapshotTerm;
    private final long snapshotIndex;

    private CloseableKVStore snapshot;                              // snapshot view of key/value store
    private Iterator<KVPair> iterator;

    private long pairIndex;                                         // count of how many key/value pairs sent so far
    private KVPair nextPair;
    private byte[] previousKey;

// Constructors

    public SnapshotTransmit(long snapshotTerm, long snapshotIndex, CloseableKVStore snapshot, byte[] keyPrefix) {
        this.snapshotTerm = snapshotTerm;
        this.snapshotIndex = snapshotIndex;
        this.snapshot = snapshot;
        this.iterator = PrefixKVStore.create(this.snapshot, keyPrefix).getRange(null, null, false);
        this.advance();
    }

// Public methods

    /**
     * Get the age of this instance since instantiation.
     *
     * @return age in milliseconds
     */
    public int getAge() {
        return -this.createTime.offsetFromNow();
    }

    public long getSnapshotTerm() {
        return this.snapshotTerm;
    }

    public long getSnapshotIndex() {
        return this.snapshotIndex;
    }

    public long getPairIndex() {
        return this.pairIndex;
    }

    public boolean hasMoreChunks() {
        return this.nextPair != null;
    }

    public ByteBuffer getNextChunk() {

        // Any more key/value pairs?
        if (this.nextPair == null)
            return null;

        // Allocate buffer
        final ByteBuffer buf = Util.allocateByteBuffer(Math.max(this.nextPairLength(), MAX_CHUNK_SIZE));

        // Fill buffer with the next chunk of key/value pairs
        final ByteBufferOutputStream output = new ByteBufferOutputStream(buf);
        do {
            final byte[] key = this.nextPair.getKey();
            final byte[] value = this.nextPair.getValue();
            try {
                KeyListEncoder.write(output, key, this.previousKey);
                KeyListEncoder.write(output, value, null);
            } catch (IOException e) {
                throw new RuntimeException("unexpected exception");
            }
            this.previousKey = key;
            this.pairIndex++;
        } while (this.advance() && buf.remaining() >= this.nextPairLength());

        // Done
        return (ByteBuffer)buf.flip();
    }

// Private methods

    private boolean advance() {
        if (!this.iterator.hasNext()) {
            this.nextPair = null;
            return false;
        }
        this.nextPair = this.iterator.next();
        return true;
    }

    private int nextPairLength() {
        return KeyListEncoder.writeLength(this.nextPair.getKey(), this.previousKey)
             + KeyListEncoder.writeLength(this.nextPair.getValue(), null);
    }

// Closeable

    @Override
    public void close() {
        Util.closeIfPossible(this.snapshot);
        Util.closeIfPossible(this.iterator);
        this.snapshot = null;
        this.iterator = null;
        this.nextPair = null;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[snapshotTerm=" + this.snapshotTerm
          + ",snapshotIndex=" + this.snapshotIndex
          + ",pairIndex=" + this.pairIndex
          + (this.snapshot == null ? ",closed" : "")
          + "]";
    }
}

