
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Map;

import org.jsimpledb.util.ByteUtil;

/**
 * A key/value pair.
 *
 * Note: the internal byte arrays are not copied; therefore, values passed to the constructor
 * or returned from the accessor methods must not be modified if instances are to remain immutable.
 */
public class KVPair {

    private final byte[] key;
    private final byte[] value;

    /**
     * Constructor. The given arrays are copied.
     *
     * @param key key
     * @param value value
     * @throws IllegalArgumentException if {@code key} or {@code value} is null
     */
    public KVPair(byte[] key, byte[] value) {
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        this.key = key;
        this.value = value;
    }

    /**
     * Constructor. The given key and value arrays are copied.
     *
     * @param entry map entry
     * @throws IllegalArgumentException if {@code entry} or its key or value is null
     */
    public KVPair(Map.Entry<byte[], byte[]> entry) {
        Preconditions.checkArgument(entry != null, "null entry");
        this.key = entry.getKey();
        this.value = entry.getValue();
        Preconditions.checkArgument(this.key != null, "null key");
        Preconditions.checkArgument(this.value != null, "null value");
    }

    /**
     * Get the key.
     *
     * @return the key
     */
    public byte[] getKey() {
        return this.key;
    }

    /**
     * Get the value.
     *
     * @return the value
     */
    public byte[] getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return "{" + ByteUtil.toString(this.key) + "," + ByteUtil.toString(this.value) + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final KVPair that = (KVPair)obj;
        return Arrays.equals(this.key, that.key) && Arrays.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.key) ^ Arrays.hashCode(this.value);
    }
}

