
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.SimpleKeyRanges;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * A mutable {@link java.util.NavigableMap} view of the keys and values in a {@link KVStore}.
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
        this(kv, false, null, new Bounds<byte[]>());
    }

    /**
     * Constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefix prefix defining minimum and maximum keys
     * @throws NullPointerException if {@code prefix} is null
     */
    public KVNavigableMap(KVStore kv, byte[] prefix) {
        this(kv, false, SimpleKeyRanges.forPrefix(prefix), KVNavigableSet.createBounds(prefix));
    }

    /**
     * Internal constructor. Used for creating sub-maps and reversed views.
     *
     * @param kv underlying {@link KVStore}
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param keyRanges visible keys, or null for no restrictions
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected KVNavigableMap(KVStore kv, boolean reversed, KeyRanges keyRanges, Bounds<byte[]> bounds) {
        super(kv, false, reversed, keyRanges, bounds);
    }

    @Override
    public Comparator<byte[]> comparator() {
        return this.reversed ? Collections.reverseOrder(ByteUtil.COMPARATOR) : ByteUtil.COMPARATOR;
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        if (!this.isVisible(key))
            throw new IllegalArgumentException("key is out of range");
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
    public NavigableSet<byte[]> navigableKeySet() {
        return new KVNavigableSet(this.kv, this.reversed, this.keyRanges, this.bounds);
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
        if (!(obj instanceof byte[]))
            throw new IllegalArgumentException("key is not a byte[]");
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
    protected NavigableMap<byte[], byte[]> createSubMap(boolean newReversed, KeyRanges newKeyRanges, Bounds<byte[]> newBounds) {
        return new KVNavigableMap(this.kv, newReversed, newKeyRanges, newBounds);
    }
}

