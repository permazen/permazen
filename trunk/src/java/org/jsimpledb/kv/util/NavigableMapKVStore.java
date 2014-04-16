
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.util.ByteUtil;

/**
 * Provides a {@link org.jsimpledb.kv.CountingKVStore} view of an underlying {@link NavigableMap NavigableMap<byte[], byte[]>}
 * whose keys are sorted lexicographically as unsigned bytes.
 */
public class NavigableMapKVStore extends CountingKVStoreAdapter {

    private final NavigableMap<byte[], byte[]> map;

    /**
     * Convenience constructor. Uses an internally constructed {@link TreeMap}.
     *
     * <p>
     * Equivalent to:
     * <blockquote><code>
     * NavigableMapKVStore(new TreeMap&lt;byte[], byte[]&gt;(ByteUtil.COMPARATOR)
     * </code></blockquote>
     * </p>
     *
     * @see org.jsimpledb.util.ByteUtil#COMPARATOR
     */
    public NavigableMapKVStore() {
        this(new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR));
    }

    /**
     * Primary constructor.
     *
     * <p>
     * The underlying map <b>must</b> sort keys lexicographically as unsigned bytes; otherwise, behavior is undefined.
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

