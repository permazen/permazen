
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.Bounds;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableSet;

/**
 * A {@link java.util.NavigableSet} view of the keys in a {@link KVStore}.
 *
 * <p>
 * Instances are mutable, with these exceptions:
 * <ul>
 *  <li>Adding new elements via {@link #add add()} is not supported</li>
 *  <li>{@link #clear} is not supported when a {@link KeyFilter} is configured</li>
 * </ul>
 */
@SuppressWarnings("serial")
public class KVNavigableSet extends AbstractKVNavigableSet<byte[]> {

// Constructors

    /**
     * Constructor for when there are no range restrictions.
     *
     * @param kv underlying {@link KVStore}
     */
    public KVNavigableSet(KVStore kv) {
        this(kv, false, null, null);
    }

    /**
     * Constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefix prefix defining minimum and maximum keys
     * @throws NullPointerException if {@code prefix} is null
     */
    public KVNavigableSet(KVStore kv, byte[] prefix) {
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
    protected KVNavigableSet(KVStore kv, boolean reversed, KeyRange keyRange, KeyFilter keyFilter) {
        this(kv, reversed, keyRange, keyFilter, KVNavigableSet.createBounds(keyRange));
    }

    /**
     * Internal constructor. Used for creating sub-sets and reversed views.
     *
     * @param kv underlying {@link KVStore}
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param keyRange key range restriction, or null for none
     * @param keyFilter key filter, or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code kv} or {@code bounds} is null
     */
    private KVNavigableSet(KVStore kv, boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<byte[]> bounds) {
        super(kv, false, reversed, keyRange, keyFilter, bounds);
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
        if (!this.isVisible(key))
            return false;
        final byte[] value = this.kv.get(key);
        if (value == null)
            return false;
        this.kv.remove(key);
        return true;
    }

    @Override
    public void clear() {
        if (this.keyFilter != null)
            throw new UnsupportedOperationException("clear() not supported when KeyFilter configured");
        final byte[] minKey = this.keyRange != null ? this.keyRange.getMin() : null;
        final byte[] maxKey = this.keyRange != null ? this.keyRange.getMax() : null;
        this.kv.removeRange(minKey, maxKey);
        return;
    }

    @Override
    protected void encode(ByteWriter writer, Object obj) {
        Preconditions.checkArgument(obj instanceof byte[], "value is not a byte[]");
        writer.write((byte[])obj);
    }

    @Override
    protected byte[] decode(ByteReader reader) {
        return reader.getBytes();
    }

    @Override
    protected NavigableSet<byte[]> createSubSet(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<byte[]> newBounds) {
        return new KVNavigableSet(this.kv, newReversed, newKeyRange, newKeyFilter, newBounds);
    }

    static Bounds<byte[]> createBounds(KeyRange keyRange) {
        return keyRange != null ? new Bounds<>(keyRange.getMin(), keyRange.getMax()) : new Bounds<>();
    }
}
