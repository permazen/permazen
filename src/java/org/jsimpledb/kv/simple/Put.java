
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.simple;

import com.google.common.base.Preconditions;

import org.jsimpledb.kv.KVStore;

/**
 * Represents the addition or changing of a key/value pair in a {@link SimpleKVTransaction}.
 *
 * <p>
 * Note the definition of {@linkplain #equals equality} does <b>not</b> include the {@linkplain #getValue value}.
 * </p>
 */
class Put extends Mutation {

    private final byte[] value;

    public Put(byte[] key, byte[] value) {
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

    @Override
    public void apply(KVStore kv) {
        kv.put(this.getKey(), this.getValue());
    }
}

