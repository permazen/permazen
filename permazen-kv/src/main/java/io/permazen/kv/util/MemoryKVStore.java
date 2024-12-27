
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a {@link KVStore} view of an underlying {@link ConcurrentNavigableMap ConcurrentNavigableMap&lt;ByteData, ByteData&gt;}.
 *
 * <p>
 * Implementaions are serializable if the underlying map is.
 */
@ThreadSafe
public class MemoryKVStore extends AbstractKVStore implements Cloneable, Serializable {

    private static final long serialVersionUID = -8112493152056118516L;

    @SuppressWarnings("serial")
    protected /*final*/ ConcurrentNavigableMap<ByteData, ByteData> map;

    /**
     * Convenience constructor.
     *
     * <p>
     * Uses an internally constructed {@link ConcurrentSkipListMap}.
     *
     * <p>
     * Equivalent to:
     * <blockquote><pre>
     * MemoryKVStore(new ConcurrentSkipListMap&lt;ByteData, ByteData&gt;()
     * </pre></blockquote>
     */
    public MemoryKVStore() {
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
    public MemoryKVStore(ConcurrentNavigableMap<ByteData, ByteData> map) {
        Preconditions.checkArgument(map != null, "null map");
        Preconditions.checkArgument(map.comparator() == null
          || map.comparator().compare(ByteData.fromHex("00"), ByteData.fromHex("ff")) < 0, "invalid comparator");
        this.map = map;
    }

    /**
     * Get the underlying {@link ConcurrentNavigableMap} used by this instance.
     *
     * @return the underlying map
     */
    public ConcurrentNavigableMap<ByteData, ByteData> getNavigableMap() {
        return this.map;
    }

// KVStore

    @Override
    public ByteData get(ByteData key) {
        Preconditions.checkArgument(key != null, "null key");
        return this.map.get(key);
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        final Map.Entry<ByteData, ByteData> entry = minKey != null ? this.map.ceilingEntry(minKey) : this.map.firstEntry();
        return entry != null && (maxKey == null || entry.getKey().compareTo(maxKey) < 0) ?
          new KVPair(entry.getKey(), entry.getValue()) : null;
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        final Map.Entry<ByteData, ByteData> entry = maxKey != null ? this.map.lowerEntry(maxKey) : this.map.lastEntry();
        return entry != null && (minKey == null || entry.getKey().compareTo(minKey) >= 0) ?
          new KVPair(entry.getKey(), entry.getValue()) : null;
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        NavigableMap<ByteData, ByteData> rangeMap = this.map;
        if (minKey != null && maxKey != null) {
            Preconditions.checkArgument(minKey.compareTo(maxKey) <= 0, "minKey > maxKey");
            rangeMap = rangeMap.subMap(minKey, true, maxKey, false);
        } else if (minKey != null)
            rangeMap = rangeMap.tailMap(minKey, true);
        else if (maxKey != null)
            rangeMap = rangeMap.headMap(maxKey, false);
        if (reverse)
            rangeMap = rangeMap.descendingMap();
        return CloseableIterator.wrap(Iterators.transform(rangeMap.entrySet().iterator(),
          entry -> new KVPair(entry.getKey(), entry.getValue())));
    }

    @Override
    public void put(ByteData key, ByteData value) {
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        this.map.put(key, value);
    }

    @Override
    public void remove(ByteData key) {
        Preconditions.checkArgument(key != null, "null key");
        this.map.remove(key);
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        if (minKey == null && maxKey == null)
            this.map.clear();
        else if (minKey == null)
            this.map.headMap(maxKey).clear();
        else if (maxKey == null)
            this.map.tailMap(minKey).clear();
        else
            this.map.subMap(minKey, maxKey).clear();
    }

// Cloneable

    @Override
    public MemoryKVStore clone() {
        final MemoryKVStore clone;
        try {
            clone = (MemoryKVStore)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        if (clone.map instanceof ConcurrentSkipListMap)
            clone.map = ((ConcurrentSkipListMap<ByteData, ByteData>)clone.map).clone();
        else
            clone.map = new ConcurrentSkipListMap<>(clone.map);
        return clone;
    }
}
