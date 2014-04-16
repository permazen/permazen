
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import org.jsimpledb.kv.CountingKVStore;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * Adpater superclass that implements {@link CountingKVStore} using a simple big-endian encoding.
 */
public abstract class CountingKVStoreAdapter implements CountingKVStore {

    protected CountingKVStoreAdapter() {
    }

    @Override
    public byte[] encodeCounter(long value) {
        final ByteWriter writer = new ByteWriter(8);
        ByteUtil.writeLong(writer, value);
        return writer.getBytes();
    }

    @Override
    public long decodeCounter(byte[] value) {
        if (value.length != 8)
            throw new IllegalArgumentException("invalid encoded counter value: length = " + value.length + " != 8");
        return ByteUtil.readLong(new ByteReader(value));
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        if (key == null)
            throw new NullPointerException("null key");
        byte[] previous = this.get(key);
        if (previous == null)
            previous = new byte[8];
        this.put(key, this.encodeCounter(this.decodeCounter(previous) + amount));
    }
}

