
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Provides a {@link org.jsimpledb.kv.KVStore} view of an underlying
 * {@link ConcurrentSkipListMap NavigableMap&lt;byte[], byte[]&gt;} whose keys are sorted lexicographically as unsigned bytes.
 */
public class NavigableMapKVStore extends AbstractKVStore implements Cloneable {

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
     * @param map underlying map
     * @throws IllegalArgumentException if {@code map} is null
     * @throws IllegalArgumentException if an invalid comparator is detected (this check is not reliable)
     * @see org.jsimpledb.util.ByteUtil#COMPARATOR
     */
    public NavigableMapKVStore(ConcurrentSkipListMap<byte[], byte[]> map) {
        Preconditions.checkArgument(map != null, "null map");
        Preconditions.checkArgument(map.comparator() != null
          && map.comparator().compare(ByteUtil.parse("00"), ByteUtil.parse("ff")) < 0, "invalid comparator");
        this.map = map;
        synchronized (this) { }
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

// Encoding/Decoding

    /**
     * Determine the number of bytes that would be written by {@link #encode encode()}.
     *
     * @return encoded length of this instance
     */
    public long encodedLength() {
        long total = UnsignedIntEncoder.encodeLength(this.size());
        byte[] prev = null;
        for (Iterator<KVPair> i = this.getRange(null, null, false); i.hasNext(); ) {
            final KVPair kv = i.next();
            final byte[] key = kv.getKey();
            final byte[] value = kv.getValue();
            total += KeyListEncoder.writeLength(key, prev);
            total += KeyListEncoder.writeLength(value, null);
            prev = key;
        }
        return total;
    }

    /**
     * Encode this instance into bytes.
     *
     * @param output encoded output
     * @throws IOException if an I/O error occurs
     */
    public void encode(OutputStream output) throws IOException {
        Preconditions.checkArgument(output != null, "null output");
        UnsignedIntEncoder.write(output, this.size());
        byte[] prev = null;
        for (Iterator<KVPair> i = this.getRange(null, null, false); i.hasNext(); ) {
            final KVPair kv = i.next();
            final byte[] key = kv.getKey();
            final byte[] value = kv.getValue();
            KeyListEncoder.write(output, key, prev);
            KeyListEncoder.write(output, value, null);
            prev = key;
        }
    }

    /**
     * Decode a {@link NavigableMapKVStore} previously encoded by {@link #encode encode()}.
     *
     * @param input encoded input
     * @return decoded {@link NavigableMapKVStore}
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code input} is invalid
     * @throws IllegalArgumentException if {@code input} contains invalid content
     */
    public static NavigableMapKVStore decode(InputStream input) throws IOException {
        Preconditions.checkArgument(input != null, "null input");
        final NavigableMapKVStore kvstore = new NavigableMapKVStore();
        int count = UnsignedIntEncoder.read(input);
        byte[] prev = null;
        for (int i = 0; i < count; i++) {
            final byte[] key = KeyListEncoder.read(input, prev);
            final byte[] value = KeyListEncoder.read(input, null);
            if (prev != null && ByteUtil.compare(key, prev) <= 0) {
                throw new IllegalArgumentException("read out-of-order key "
                  + ByteUtil.toString(key) + " <= " + ByteUtil.toString(prev));
            }
            kvstore.put(key, value);
            prev = key;
        }
        return kvstore;
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
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
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        this.map.put(key, value);
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

