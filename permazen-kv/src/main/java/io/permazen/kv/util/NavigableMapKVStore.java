
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.concurrent.ThreadSafe;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.KVPair;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

/**
 * Provides a {@link io.permazen.kv.KVStore} view of an underlying
 * {@link ConcurrentSkipListMap NavigableMap&lt;byte[], byte[]&gt;} whose keys are sorted lexicographically as unsigned bytes.
 */
@ThreadSafe
public class NavigableMapKVStore extends AbstractKVStore implements Cloneable, Serializable {

    private static final long serialVersionUID = -8112493152056118516L;

    private /*final*/ ConcurrentSkipListMap<byte[], byte[]> map;

    /**
     * Convenience constructor. Uses an internally constructed {@link ConcurrentSkipListMap}.
     *
     * <p>
     * Equivalent to:
     * <blockquote><code>
     * NavigableMapKVStore(new ConcurrentSkipListMap&lt;byte[], byte[]&gt;(ByteUtil.COMPARATOR)
     * </code></blockquote>
     *
     * @see io.permazen.util.ByteUtil#COMPARATOR
     */
    public NavigableMapKVStore() {
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
     * @see io.permazen.util.ByteUtil#COMPARATOR
     */
    public NavigableMapKVStore(ConcurrentSkipListMap<byte[], byte[]> map) {
        Preconditions.checkArgument(map != null, "null map");
        Preconditions.checkArgument(map.comparator() != null
          && map.comparator().compare(ByteUtil.parse("00"), ByteUtil.parse("ff")) < 0, "invalid comparator");
        this.map = map;
    }

    /**
     * Get the underlying {@link NavigableMap} used by this instance.
     *
     * @return the underlying map
     */
    public ConcurrentSkipListMap<byte[], byte[]> getNavigableMap() {
        return this.map;
    }

    /**
     * Get the number of key/value pairs in this instance.
     *
     * @return size of this instance
     */
    public int size() {
        return this.map.size();
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
        final byte[] value = this.map.get(key);
        return value != null ? value.clone() : null;
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        final Map.Entry<byte[], byte[]> entry = minKey != null ? this.map.ceilingEntry(minKey) : this.map.firstEntry();
        return entry != null && (maxKey == null || ByteUtil.compare(entry.getKey(), maxKey) < 0) ?
          new KVPair(entry.getKey().clone(), entry.getValue().clone()) : null;
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        final Map.Entry<byte[], byte[]> entry = maxKey != null ? this.map.lowerEntry(maxKey) : this.map.lastEntry();
        return entry != null && (minKey == null || ByteUtil.compare(entry.getKey(), minKey) >= 0) ?
          new KVPair(entry.getKey().clone(), entry.getValue().clone()) : null;
    }

    @Override
    public CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        NavigableMap<byte[], byte[]> rangeMap = this.map;
        if (minKey != null && maxKey != null)
            rangeMap = rangeMap.subMap(minKey, true, maxKey, false);
        else if (minKey != null)
            rangeMap = rangeMap.tailMap(minKey, true);
        else if (maxKey != null)
            rangeMap = rangeMap.headMap(maxKey, false);
        if (reverse)
            rangeMap = rangeMap.descendingMap();
        return CloseableIterator.wrap(Iterators.transform(rangeMap.entrySet().iterator(),
          entry -> new KVPair(entry.getKey().clone(), entry.getValue().clone())));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        this.map.put(key.clone(), value.clone());
    }

    @Override
    public void remove(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
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

// Cloneable

    @Override
    public NavigableMapKVStore clone() {
        final NavigableMapKVStore clone;
        try {
            clone = (NavigableMapKVStore)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.map = this.map.clone();
        return clone;
    }
}

