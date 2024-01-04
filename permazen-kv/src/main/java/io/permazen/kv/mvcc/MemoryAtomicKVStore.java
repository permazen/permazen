
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ImmutableNavigableMap;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a {@link KVStore} view of an underlying {@link ConcurrentNavigableMap ConcurrentNavigableMap&lt;byte[], byte[]&gt;}
 * whose keys are sorted lexicographically as unsigned bytes.
 *
 * <p>
 * Implementaions are serializable if the underlying map is.
 */
@ThreadSafe
public class MemoryAtomicKVStore extends MemoryKVStore implements AtomicKVStore {

    private static final long serialVersionUID = -1764030312068037867L;

    /**
     * Convenience constructor. Uses an internally constructed {@link ConcurrentSkipListMap}.
     *
     * <p>
     * Equivalent to:
     * <blockquote><pre>
     * MemoryAtomicKVStore(new ConcurrentSkipListMap&lt;byte[], byte[]&gt;(ByteUtil.COMPARATOR)
     * </pre></blockquote>
     *
     * @see ByteUtil#COMPARATOR
     */
    public MemoryAtomicKVStore() {
        this(new ConcurrentSkipListMap<>(ByteUtil.COMPARATOR));
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
     * @see ByteUtil#COMPARATOR
     */
    public MemoryAtomicKVStore(ConcurrentNavigableMap<byte[], byte[]> map) {
        super(map);
    }

// KVStore

    @Override
    public synchronized byte[] get(byte[] key) {
        return super.get(key);
    }

    @Override
    public synchronized KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        return super.getAtLeast(minKey, maxKey);
    }

    @Override
    public synchronized KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        return super.getAtMost(maxKey, minKey);
    }

    @Override
    public synchronized CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        return super.getRange(minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        super.put(key, value);
    }

    @Override
    public synchronized void remove(byte[] key) {
        super.remove(key);
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {
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

        SnapshotKVStore(NavigableMap<byte[], byte[]> map) {
            super(new MemoryAtomicKVStore(new ImmutableNavigableMap<byte[], byte[]>(map, ByteUtil.COMPARATOR)), false);
        }

        @Override
        public void close() {
        }
    }
}
