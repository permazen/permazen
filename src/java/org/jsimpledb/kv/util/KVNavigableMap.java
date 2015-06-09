
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableMap;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * A {@link java.util.NavigableMap} view of the keys and values in a {@link KVStore}.
 *
 * Instances are mutable, with these exceptions:
 * <ul>
 *  <li>{@link #clear} is not supported when a {@link KeyFilter} is configured</li>
 * </ul>
 */
@SuppressWarnings("serial")
public class KVNavigableMap extends AbstractKVNavigableMap<byte[], byte[]> {

// Constructors

    /**
     * Constructor for when there are no range restrictions.
     *
     * @param kv underlying {@link KVStore}
     */
    public KVNavigableMap(KVStore kv) {
        this(kv, false, null, null);
    }

    /**
     * Constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefix prefix defining minimum and maximum keys
     * @throws NullPointerException if {@code prefix} is null
     */
    public KVNavigableMap(KVStore kv, byte[] prefix) {
        this(kv, false, KeyRange.forPrefix(prefix), null);
    }

    /**
     * Primary constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param reversed whether ordering is reversed
     * @param keyRange key range restriction, or null for none
     * @param keyFilter key filter, or null for none
     * @throws IllegalArgumentException if {@code kv} is null
     */
    protected KVNavigableMap(KVStore kv, boolean reversed, KeyRange keyRange, KeyFilter keyFilter) {
        this(kv, reversed, keyRange, keyFilter, KVNavigableSet.createBounds(keyRange));
    }

    /**
     * Internal constructor. Used for creating sub-maps and reversed views.
     *
     * @param kv underlying {@link KVStore}
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param keyRange key range restriction, or null for none
     * @param keyFilter key filter, or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code kv} or {@code bounds} is null
     */
    private KVNavigableMap(KVStore kv, boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<byte[]> bounds) {
        super(kv, false, reversed, keyRange, keyFilter, bounds);
    }

// Methods

    @Override
    public Comparator<byte[]> comparator() {
        return this.reversed ? Collections.reverseOrder(ByteUtil.COMPARATOR) : ByteUtil.COMPARATOR;
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        Preconditions.checkArgument(this.isVisible(key), "key is out of range or filtered out");
        final byte[] previousValue = this.kv.get(key);
        this.kv.put(key, value);
        return previousValue;
    }

    @Override
    public byte[] remove(Object obj) {
        if (!(obj instanceof byte[]))
            return null;
        final byte[] key = (byte[])obj;
        if (!this.isVisible(key))
            return null;
        final byte[] previousValue = this.kv.get(key);
        if (previousValue != null)
            this.kv.remove(key);
        return previousValue;
    }

    @Override
    public void clear() {
        this.navigableKeySet().clear();
    }

    @Override
    public boolean containsKey(Object obj) {
        if (!(obj instanceof byte[]))
            return false;
        final byte[] key = (byte[])obj;
        if (!this.isVisible(key))
            return false;
        return this.kv.get(key) != null;
    }

    @Override
    protected void encodeKey(ByteWriter writer, Object obj) {
        Preconditions.checkArgument(obj instanceof byte[], "key is not a byte[]");
        writer.write((byte[])obj);
    }

    @Override
    protected byte[] decodeKey(ByteReader reader) {
        return reader.getBytes();
    }

    @Override
    protected byte[] decodeValue(KVPair pair) {
        return pair.getValue();
    }

    @Override
    protected NavigableMap<byte[], byte[]> createSubMap(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<byte[]> newBounds) {
        return new KVNavigableMap(this.kv, newReversed, newKeyRange, newKeyFilter, newBounds);
    }
}

