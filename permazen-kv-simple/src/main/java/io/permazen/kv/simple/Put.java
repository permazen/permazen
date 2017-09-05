
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.util.KeyWatchTracker;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

/**
 * Represents the addition or changing of a key/value pair in a {@link SimpleKVTransaction}.
 *
 * <p>
 * Note the definition of {@linkplain #equals equality} does <b>not</b> include the {@linkplain #getValue value}.
 */
class Put extends Mutation {

    private final byte[] value;

    Put(byte[] key, byte[] value) {
        super(key);
        Preconditions.checkArgument(value != null, "null value");
        this.value = value.clone();
    }

    public byte[] getKey() {
        return this.getMin();
    }

    public byte[] getValue() {
        return this.value.clone();
    }

    public Map.Entry<byte[], byte[]> toMapEntry() {
        return new AbstractMap.SimpleEntry<>(this.getKey(), this.getValue());
    }

    @Override
    public void apply(KVStore kv) {
        kv.put(this.getKey(), this.getValue());
    }

    @Override
    public boolean trigger(KeyWatchTracker keyWatchTracker) {
        return keyWatchTracker.trigger(this.getKey());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final Put that = (Put)obj;
        return Arrays.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Arrays.hashCode(this.value);
    }
}

