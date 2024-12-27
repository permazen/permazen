
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.util.KeyWatchTracker;
import io.permazen.util.ByteData;

import java.util.AbstractMap;
import java.util.Map;

/**
 * Represents the addition or changing of a key/value pair in a {@link SimpleKVTransaction}.
 *
 * <p>
 * Note the definition of {@linkplain #equals equality} does <b>not</b> include the {@linkplain #getValue value}.
 */
class Put extends Mutation {

    private final ByteData value;

    Put(ByteData key, ByteData value) {
        super(key);
        Preconditions.checkArgument(value != null, "null value");
        this.value = value;
    }

    public ByteData getKey() {
        return this.getMin();
    }

    public ByteData getValue() {
        return this.value;
    }

    public Map.Entry<ByteData, ByteData> toMapEntry() {
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
        return this.value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.value.hashCode();
    }
}
