
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Map;

import io.permazen.util.ByteUtil;

/**
 * A key/value pair.
 *
 * <p>
 * Note: the internal byte arrays are not copied; therefore, values passed to the constructor
 * or returned from the accessor methods must not be modified if instances are to remain immutable.
 * To ensure safety, use {@link #clone}.
 */
public class KVPair implements Cloneable {

    private /*final*/ byte[] key;
    private /*final*/ byte[] value;

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

// Cloneable

    /**
     * Deep-clone this instance. Copys this instance as well as the key and value {@code byte[]} arrays.
     *
     * @return cloned instance
     */
    @Override
    public KVPair clone() {
        final KVPair clone;
        try {
            clone = (KVPair)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.key = clone.key.clone();
        clone.value = clone.value.clone();
        return clone;
    }

// Object

    @Override
    public String toString() {
        return "{" + ByteUtil.toString(this.key) + "," + ByteUtil.toString(this.value) + "}";
    }

    /**
     * Compare for equality.
     *
     * <p>
     * Two {@link KVPair} instances are equal if the keys and values both match
     * according to {@link Arrays#equals(byte[], byte[])}.
     *
     * @return true if objects are equal
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final KVPair that = (KVPair)obj;
        return Arrays.equals(this.key, that.key) && Arrays.equals(this.value, that.value);
    }

    /**
     * Calculate hash code.
     *
     * <p>
     * The hash code of a {@link KVPair} is the exclusive-OR of the hash codes of the key
     * and the value, each according to {@link Arrays#hashCode(byte[])}.
     *
     * @return hash value for this instance
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(this.key) ^ Arrays.hashCode(this.value);
    }
}

