
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;

import io.permazen.kv.KeyRange;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.raft.msg.InstallSnapshot;
import io.permazen.kv.util.KeyListEncoder;
import io.permazen.util.ByteData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Stream;

import org.dellroad.stuff.io.ByteBufferInputStream;

/**
 * Represents and in-progress snapshot installation from the follower's point of view.
 *
 * <p>
 * Instances are not thread safe.
 */
class SnapshotReceive {

    private final AtomicKVStore kv;
    private final ByteData prefix;
    private final long snapshotTerm;
    private final long snapshotIndex;
    private final Map<String, String> snapshotConfig;

    private long pairIndex;
    private ByteData previousKey;

// Constructors

    SnapshotReceive(AtomicKVStore kv, ByteData prefix, long snapshotTerm, long snapshotIndex, Map<String, String> snapshotConfig) {
        Preconditions.checkArgument(kv != null, "null kv");
        Preconditions.checkArgument(prefix != null, "null prefix");
        Preconditions.checkArgument(snapshotTerm > 0);
        Preconditions.checkArgument(snapshotIndex > 0);
        Preconditions.checkArgument(snapshotConfig != null);
        this.kv = kv;
        this.prefix = prefix;
        this.snapshotTerm = snapshotTerm;
        this.snapshotIndex = snapshotIndex;
        this.snapshotConfig = snapshotConfig;
    }

// Public methods

    public long getSnapshotTerm() {
        return this.snapshotTerm;
    }

    public long getSnapshotIndex() {
        return this.snapshotIndex;
    }

    public Map<String, String> getSnapshotConfig() {
        return this.snapshotConfig;
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

        // Read and apply key/value pairs in a single operation
        final PutMutations mutations = new PutMutations(buf, this.prefix, this.previousKey);
        this.kv.apply(mutations, false);
        assert mutations.getEndKey() != null || (this.pairIndex == 0 && mutations.getNumPuts() == 0);

        // Advance our installation frontier
        this.pairIndex += mutations.getNumPuts();
        this.previousKey = mutations.getEndKey();
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
          + ",snapshotConfig=" + this.snapshotConfig
          + ",pairIndex=" + this.pairIndex
          + "]";
    }

// PutMutations

    private static class PutMutations implements Mutations {

        private final ByteBuffer buf;
        private final ByteData prefix;
        private final ByteData startKey;

        // These fields are copied back from completed PutIterators
        private ByteData endKey;
        private int numPuts;

        PutMutations(ByteBuffer buf, ByteData prefix, ByteData startKey) {
            this.buf = buf;
            this.prefix = prefix;
            this.startKey = startKey;
        }

    // Mutations

        @Override
        public Stream<KeyRange> getRemoveRanges() {
            return Stream.empty();
        }

        @Override
        public Stream<Map.Entry<ByteData, ByteData>> getPutPairs() {
            return Streams.stream(new PutIterator(this, this.buf.asReadOnlyBuffer(), this.prefix, this.startKey));
        }

        @Override
        public Stream<Map.Entry<ByteData, Long>> getAdjustPairs() {
            return Stream.empty();
        }

    // Iteration completion writebacks

        public ByteData getEndKey() {
            return this.endKey;
        }
        public void setEndKey(ByteData endKey) {
            this.endKey = endKey;
        }

        public int getNumPuts() {
            return this.numPuts;
        }
        public void setNumPuts(int numPuts) {
            this.numPuts = numPuts;
        }
    }

// PutIterator

    private static class PutIterator extends AbstractIterator<Map.Entry<ByteData, ByteData>> {

        private final PutMutations mutations;
        private final ByteBuffer buf;
        private final ByteBufferInputStream input;
        private final ByteData prefix;

        private ByteData previousKey;
        private int numPuts;

        PutIterator(PutMutations mutations, ByteBuffer buf, ByteData prefix, ByteData startKey) {
            this.mutations = mutations;
            this.buf = buf;
            this.input = new ByteBufferInputStream(buf);
            this.prefix = prefix;
            this.previousKey = startKey;
        }

        @Override
        protected Map.Entry<ByteData, ByteData> computeNext() {

            // Check if there's more data
            if (!this.buf.hasRemaining()) {
                this.mutations.setEndKey(this.previousKey);
                this.mutations.setNumPuts(this.numPuts);
                return this.endOfData();
            }

            // Decode next key/value pair
            final ByteData key;
            final ByteData value;
            try {
                key = KeyListEncoder.read(input, this.previousKey);
                value = KeyListEncoder.read(input, null);
            } catch (IOException e) {
                throw new IllegalArgumentException("invalid encoded key/value data", e.getCause());
            }
            this.previousKey = key;
            this.numPuts++;

            // Done
            return new AbstractMap.SimpleImmutableEntry<>(this.prefix.concat(key), value);
        }
    }
}
