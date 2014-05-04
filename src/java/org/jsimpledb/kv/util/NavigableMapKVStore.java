
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.util.ByteUtil;

/**
 * Provides a {@link org.jsimpledb.kv.CountingKVStore} view of an underlying {@link NavigableMap NavigableMap<byte[], byte[]>}
 * whose keys are sorted lexicographically as unsigned bytes.
 */
public class NavigableMapKVStore extends CountingKVStoreAdapter {

    private final NavigableMap<byte[], byte[]> map;

    /**
     * Convenience constructor. Uses an internally constructed {@link ConcurrentSkipListMap}.
     *
     * <p>
     * Equivalent to:
     * <blockquote><code>
     * NavigableMapKVStore(new ConcurrentSkipListMap&lt;byte[], byte[]&gt;(ByteUtil.COMPARATOR)
     * </code></blockquote>
     * </p>
     *
     * @see org.jsimpledb.util.ByteUtil#COMPARATOR
     */
    public NavigableMapKVStore() {
        this(new ConcurrentSkipListMap<byte[], byte[]>(ByteUtil.COMPARATOR));
    }

    /**
     * Primary constructor.
     *
     * <p>
     * The underlying map <b>must</b> sort keys lexicographically as unsigned bytes; otherwise, behavior is undefined.
     * </p>
     *
     * <p>
     * The underlying map's iterator <b>must not</b> throw {@link java.util.ConcurrentModificationException}. For example,
     * {@link java.util.TreeMap} does not satisfy this constraint.
     * </p>
     *
     * @param map underlying map
     * @throws IllegalArgumentException if {@code map} is null
     * @see org.jsimpledb.util.ByteUtil#COMPARATOR
     */
    public NavigableMapKVStore(NavigableMap<byte[], byte[]> map) {
        if (map == null)
            throw new IllegalArgumentException("null map");
        this.map = map;
    }

    /**
     * Get the underlying {@link NavigableMap} used by this instance..
     *
     * @return the underlying map
     */
    public NavigableMap<byte[], byte[]> getNavigableMap() {
        return this.map;
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        if (key == null)
            throw new NullPointerException("null key");
        return this.map.get(key);
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        final Map.Entry<byte[], byte[]> entry = minKey != null ? this.map.ceilingEntry(minKey) : this.map.firstEntry();
        return entry != null ? new KVPair(entry) : null;
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        final Map.Entry<byte[], byte[]> entry = maxKey != null ? this.map.lowerEntry(maxKey) : this.map.lastEntry();
        return entry != null ? new KVPair(entry) : null;
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        NavigableMap<byte[], byte[]> rangeMap = this.map;
        if (minKey != null && maxKey != null)
            rangeMap = rangeMap.subMap(minKey, true, maxKey, false);
        else if (minKey != null)
            rangeMap = rangeMap.tailMap(minKey, true);
        else if (maxKey != null)
            rangeMap = rangeMap.headMap(maxKey, false);
        if (reverse)
            rangeMap = rangeMap.descendingMap();
        return Iterators.transform(rangeMap.entrySet().iterator(), new Function<Map.Entry<byte[], byte[]>, KVPair>() {
            @Override
            public KVPair apply(Map.Entry<byte[], byte[]> entry) {
                return new KVPair(entry.getKey(), entry.getValue());
            }
        });
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (key == null)
            throw new NullPointerException("null key");
        if (value == null)
            throw new NullPointerException("null value");
        this.map.put(key, value);
    }

    @Override
    public void remove(byte[] key) {
        if (key == null)
            throw new NullPointerException("null key");
        this.map.remove(key);
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        if (minKey == null && maxKey == null)
            this.map.clear();
        else if (minKey == null)
            this.map.headMap(maxKey).clear();
        else if (maxKey == null)
            this.map.tailMap(minKey).clear();
        else
            this.map.subMap(minKey, maxKey).clear();
    }
}

