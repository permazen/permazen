
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.util.KeyListEncoder;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.dellroad.stuff.io.ByteBufferOutputStream;

/**
 * Represents an in-progress snapshot installation from the leader's point of view.
 *
 * <p>
 * Instances are not thread safe.
 */
class SnapshotTransmit implements Closeable {

    private static final int MAX_CHUNK_SIZE = 128 * 1024;           // 128K

    private final Timestamp createTime = new Timestamp();
    private final long snapshotTerm;
    private final long snapshotIndex;
    private final Map<String, String> snapshotConfig;

    private CloseableKVStore snapshot;                              // snapshot view of key/value store
    private CloseableIterator<KVPair> iterator;

    private long pairIndex;                                         // count of how many key/value pairs sent so far
    private KVPair nextPair;
    private ByteData previousKey;
    private boolean anyChunksSent;

// Constructors

    SnapshotTransmit(final long snapshotTerm, final long snapshotIndex, final Map<String, String> snapshotConfig,
      final CloseableKVStore snapshot, final KVStore view) {
        Preconditions.checkArgument(snapshot != null);
        Preconditions.checkArgument(snapshotTerm > 0);
        Preconditions.checkArgument(snapshotIndex > 0);
        Preconditions.checkArgument(snapshotConfig != null);
        this.snapshotTerm = snapshotTerm;
        this.snapshotIndex = snapshotIndex;
        this.snapshotConfig = snapshotConfig;
        this.snapshot = snapshot;
        this.iterator = view.getRange(null, null);
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

    /**
     * Get textual description of the snapshot log entry index and term, e.g., {@code 10347t19}.
     *
     * @return description of the snapshot log entry
     */
    public String getBaseEntry() {
        return this.snapshotIndex + "t" + this.snapshotTerm;
    }

    public Map<String, String> getSnapshotConfig() {
        return this.snapshotConfig;
    }

    public long getPairIndex() {
        return this.pairIndex;
    }

    public boolean hasMoreChunks() {
        return this.nextPair != null;
    }

    public ByteBuffer getNextChunk() {

        // Any more key/value pairs?
        if (this.nextPair == null) {

            // In the case of a completely empty snapshot, ensure we send at least one (empty) chunk
            if (!this.anyChunksSent) {
                this.anyChunksSent = true;
                return ByteBuffer.allocate(0);
            }

            // Done
            return null;
        }

        // Allocate buffer
        final ByteBuffer buf = Util.allocateByteBuffer(Math.max(this.nextPairLength(), MAX_CHUNK_SIZE));

        // Fill buffer with the next chunk of key/value pairs
        final ByteBufferOutputStream output = new ByteBufferOutputStream(buf);
        do {
            final ByteData key = this.nextPair.getKey();
            final ByteData value = this.nextPair.getValue();
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
        this.anyChunksSent = true;
        return buf.flip();
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
        this.snapshot.close();
        this.iterator.close();
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
          + ",snapshotConfig=" + this.snapshotConfig
          + ",pairIndex=" + this.pairIndex
          + (this.snapshot == null ? ",closed" : "")
          + "]";
    }
}
