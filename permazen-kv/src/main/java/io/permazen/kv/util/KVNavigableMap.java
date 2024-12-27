
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;

import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableMap;

/**
 * A {@link java.util.NavigableMap} view of the keys and values in a {@link KVStore}.
 *
 * <p>
 * Instances are mutable, with these exceptions:
 * <ul>
 *  <li>{@link #clear} is not supported when a {@link KeyFilter} is configured</li>
 * </ul>
 */
@SuppressWarnings("serial")
public class KVNavigableMap extends AbstractKVNavigableMap<ByteData, ByteData> {

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
    public KVNavigableMap(KVStore kv, ByteData prefix) {
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
    private KVNavigableMap(KVStore kv, boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<ByteData> bounds) {
        super(kv, false, reversed, keyRange, keyFilter, bounds);
    }

// Methods

    @Override
    public Comparator<ByteData> comparator() {
        return this.reversed ? Collections.reverseOrder() : null;
    }

    @Override
    public ByteData put(ByteData key, ByteData value) {
        Preconditions.checkArgument(this.isVisible(key), "key is out of range or filtered out");
        final ByteData previousValue = this.kv.get(key);
        this.kv.put(key, value);
        return previousValue;
    }

    @Override
    public ByteData remove(Object obj) {
        if (!(obj instanceof ByteData))
            return null;
        final ByteData key = (ByteData)obj;
        if (!this.isVisible(key))
            return null;
        final ByteData previousValue = this.kv.get(key);
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
        if (!(obj instanceof ByteData))
            return false;
        final ByteData key = (ByteData)obj;
        if (!this.isVisible(key))
            return false;
        return this.kv.get(key) != null;
    }

    @Override
    protected void encodeKey(ByteData.Writer writer, Object obj) {
        Preconditions.checkArgument(obj instanceof ByteData, "key is not a ByteData");
        writer.write((ByteData)obj);
    }

    @Override
    protected ByteData decodeKey(ByteData.Reader reader) {
        return reader.readRemaining();
    }

    @Override
    protected ByteData decodeValue(KVPair pair) {
        return pair.getValue();
    }

    @Override
    protected NavigableMap<ByteData, ByteData> createSubMap(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<ByteData> newBounds) {
        return new KVNavigableMap(this.kv, newReversed, newKeyRange, newKeyFilter, newBounds);
    }
}
