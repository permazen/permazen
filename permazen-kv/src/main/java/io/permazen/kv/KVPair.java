
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;

import java.util.Map;

/**
 * A key/value pair.
 *
 * <p>
 * Instances are immutable and thread-safe.
 */
public class KVPair {

    private final ByteData key;
    private final ByteData value;

    /**
     * Constructor.
     *
     * @param key key
     * @param value value
     * @throws IllegalArgumentException if {@code key} or {@code value} is null
     */
    public KVPair(ByteData key, ByteData value) {
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        this.key = key;
        this.value = value;
    }

    /**
     * Constructor.
     *
     * @param entry map entry containing {@code byte[]} arrays
     * @throws IllegalArgumentException if {@code entry} or its key or value is null
     */
    public KVPair(Map.Entry<byte[], byte[]> entry) {
        Preconditions.checkArgument(entry != null, "null entry");
        final byte[] key = entry.getKey();
        final byte[] value = entry.getValue();
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        this.key = ByteData.of(key);
        this.value = ByteData.of(value);
    }

    /**
     * Get the key.
     *
     * @return the key
     */
    public ByteData getKey() {
        return this.key;
    }

    /**
     * Get the value.
     *
     * @return the value
     */
    public ByteData getValue() {
        return this.value;
    }

// Object

    @Override
    public String toString() {
        return "{" + this.key.toHex(64) + "," + this.value.toHex(64) + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final KVPair that = (KVPair)obj;
        return this.key.equals(that.key) && this.value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return this.key.hashCode() ^ this.value.hashCode();
    }
}
