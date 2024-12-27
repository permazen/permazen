
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ImmutableNavigableMap;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides an {@link AtomicKVStore} view of an underlying
 * {@link ConcurrentNavigableMap ConcurrentNavigableMap&lt;ByteData, ByteData&gt;}.
 *
 * <p>
 * Implementaions are serializable if the underlying map is.
 */
@ThreadSafe
public class MemoryAtomicKVStore extends MemoryKVStore implements AtomicKVStore {

    private static final long serialVersionUID = -1764030312068037867L;

    /**
     * Convenience constructor.
     *
     * <p>
     * Uses an internally constructed {@link ConcurrentSkipListMap}.
     *
     * <p>
     * Equivalent to:
     * <blockquote><pre>
     * MemoryAtomicKVStore(new ConcurrentSkipListMap&lt;ByteData, ByteData&gt;()
     * </pre></blockquote>
     */
    public MemoryAtomicKVStore() {
        this(new ConcurrentSkipListMap<>());
    }

    /**
     * Primary constructor.
     *
     * <p>
     * The underlying map <b>must</b> sort keys lexicographically as unsigned bytes; otherwise, behavior is undefined.
     *
     * @param map underlying map
     * @throws IllegalArgumentException if {@code map} is null
     * @throws IllegalArgumentException if an invalid comparator is detected (this check is not reliable)
     */
    public MemoryAtomicKVStore(ConcurrentNavigableMap<ByteData, ByteData> map) {
        super(map);
    }

// KVStore

    @Override
    public synchronized ByteData get(ByteData key) {
        return super.get(key);
    }

    @Override
    public synchronized KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        return super.getAtLeast(minKey, maxKey);
    }

    @Override
    public synchronized KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        return super.getAtMost(maxKey, minKey);
    }

    @Override
    public synchronized CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        return super.getRange(minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(ByteData key, ByteData value) {
        super.put(key, value);
    }

    @Override
    public synchronized void remove(ByteData key) {
        super.remove(key);
    }

    @Override
    public synchronized void removeRange(ByteData minKey, ByteData maxKey) {
        super.removeRange(minKey, maxKey);
    }

// AtomicKVStore

    @Override
    public synchronized void start() {
    }

    @Override
    public synchronized void stop() {
    }

    @Override
    public synchronized CloseableKVStore readOnlySnapshot() {
        return new SnapshotKVStore(this.map);
    }

    @Override
    public synchronized void apply(Mutations mutations, boolean sync) {
        super.apply(mutations);
    }

// SnapshotKVStore

    private static class SnapshotKVStore extends MutableView implements CloseableKVStore {

        SnapshotKVStore(NavigableMap<ByteData, ByteData> map) {
            super(new MemoryAtomicKVStore(new ImmutableNavigableMap<ByteData, ByteData>(map)), false);
        }

        @Override
        public void close() {
        }
    }
}
