
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableSet;

import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.util.BoundType;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * A {@link java.util.NavigableSet} view of the keys in a {@link KVStore}.
 *
 * <p>
 * Instances are mutable, with the exception that adding new elements is not supported.
 * </p>
 */
@SuppressWarnings("serial")
public class KVNavigableSet extends AbstractKVNavigableSet<byte[]> {

// Constructors

    /**
     * Convenience constructor for when there are no range restrictions.
     *
     * @param kv underlying {@link KVStore}
     */
    public KVNavigableSet(KVStore kv) {
        this(kv, (KeyRanges)null);
    }

    /**
     * Convenience constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefix prefix defining minimum and maximum keys
     * @throws NullPointerException if {@code prefix} is null
     */
    public KVNavigableSet(KVStore kv, byte[] prefix) {
        this(kv, KeyRanges.forPrefix(prefix));
    }

    /**
     * Primary constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param keyRanges visible keys, or null for no restrictions
     */
    public KVNavigableSet(KVStore kv, KeyRanges keyRanges) {
        this(kv, false, keyRanges, KVNavigableSet.createBounds(keyRanges));
    }

    /**
     * Internal constructor. Used for creating sub-sets and reversed views.
     *
     * @param kv underlying {@link KVStore}
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param keyRanges visible keys, or null for no restrictions
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected KVNavigableSet(KVStore kv, boolean reversed, KeyRanges keyRanges, Bounds<byte[]> bounds) {
        super(kv, false, reversed, keyRanges, bounds);
    }

// Methods

    @Override
    public Comparator<byte[]> comparator() {
        return this.reversed ? Collections.reverseOrder(ByteUtil.COMPARATOR) : ByteUtil.COMPARATOR;
    }

    @Override
    public boolean remove(Object obj) {
        if (!(obj instanceof byte[]))
            return false;
        final byte[] key = (byte[])obj;
        final byte[] value = this.kv.get(key);
        if (value == null)
            return false;
        this.kv.remove(key);
        return true;
    }

    @Override
    public void clear() {
        if (this.keyRanges == null) {
            this.kv.removeRange(null, null);
            return;
        }
        for (KeyRange range : this.keyRanges.getKeyRanges())
            this.kv.removeRange(range.getMin(), range.getMax());
    }

    @Override
    protected void encode(ByteWriter writer, Object obj) {
        if (!(obj instanceof byte[]))
            throw new IllegalArgumentException("value is not a byte[]");
        writer.write((byte[])obj);
    }

    @Override
    protected byte[] decode(ByteReader reader) {
        return reader.getBytes();
    }

    @Override
    protected NavigableSet<byte[]> createSubSet(boolean newReversed, KeyRanges newKeyRanges, Bounds<byte[]> newBounds) {
        return new KVNavigableSet(this.kv, newReversed, newKeyRanges, newBounds);
    }

    static Bounds<byte[]> createBounds(KeyRanges keyRanges) {
        if (keyRanges == null)
            return new Bounds<byte[]>();
        final byte[] minKey = keyRanges.getMin();
        final byte[] maxKey = keyRanges.getMax();
        final BoundType minBoundType = minKey != null ? BoundType.INCLUSIVE : BoundType.NONE;
        final BoundType maxBoundType = maxKey != null ? BoundType.EXCLUSIVE : BoundType.NONE;
        return new Bounds<byte[]>(minKey, minBoundType, maxKey, maxBoundType);
    }
}

