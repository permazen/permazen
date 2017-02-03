
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.util.ByteUtil;

/**
 * Performs searches into an {@link ArrayKVStore}.
 *
 * <p>
 * Instances are not thread safe.
 */
class ArrayKVFinder {

    private final ByteBuffer indx;
    private final ByteBuffer keys;
    private final ByteBuffer vals;
    private final int size;

    ArrayKVFinder(ByteBuffer indx, ByteBuffer keys, ByteBuffer vals) {
        Preconditions.checkArgument(indx.capacity() % 8 == 0, "index size is not a multiple of 8");
        this.indx = indx.duplicate();
        this.keys = keys.duplicate();
        this.vals = vals.duplicate();
        this.indx.limit(this.indx.capacity());
        this.keys.limit(this.keys.capacity());
        this.vals.limit(this.vals.capacity());
        this.size = this.indx.capacity() / 8;
    }

    /**
     * Search for the index of the entry with the given key.
     *
     * @return maching index, or ones complement of insertion point if not found
     */
    public int find(byte[] searchKey) {

        // Initialize bounds using cached prefix
        int min = 0;
        int max = this.size;

        // Perform binary search for key, starting at the point where we diverged from the previous search key
        byte[] prevMin = null;
        byte[] prevMax = null;
        while (min < max) {

            // Calculate the midpoint of the search range
            final int mid = (min + (max - 1)) >>> 1;

            // Get key at midpoint
            final byte[] midKey = this.readKey(mid);
            assert prevMin == null || ByteUtil.compare(searchKey, prevMin) > 0;
            assert prevMax == null || ByteUtil.compare(searchKey, prevMax) < 0;

            // Compare search key to the midpoint key
            final int diff = ByteUtil.compare(searchKey, midKey);
            if (diff == 0)
                return mid;
            if (diff < 0) {
                prevMax = midKey;
                max = mid;
            } else {
                prevMin = midKey;
                min = mid + 1;
            }
        }

        // Key was not found
        return ~min;
    }

    /**
     * Read the key at the specified index.
     */
    public byte[] readKey(int index) {

        // Sanity check
        Preconditions.checkArgument(index >= 0, "index < 0");
        Preconditions.checkArgument(index < this.size, "index >= size");

        // If this is a base key, read absolute offset and fetch data normally
        final int baseIndex = index & ~0x1f;
        final int baseKeyOffset = this.indx.getInt(baseIndex * 8);
        if (index == baseIndex) {
            final int length = (index + 1) < this.size ?
              this.indx.getInt((index + 1) * 8) & 0x00ffffff : this.keys.capacity() - baseKeyOffset;
            final byte[] data = new byte[length];
            this.keys.position(baseKeyOffset);
            this.keys.get(data);
            return data;
        }

        // Read the base key absolute offset, then encoded key prefix length and relative suffix offset
        final int encodedValue = this.indx.getInt(index * 8);
        final int prefixLen = encodedValue >>> 24;
        final int suffixOffset = baseKeyOffset + (encodedValue & 0x00ffffff);

        // Calculate the start of the following key in order to determine this key's suffix length
        final int nextIndex = index + 1;
        int nextOffset;
        if (nextIndex < this.size) {
            nextOffset = this.indx.getInt(nextIndex * 8);
            if ((nextIndex & 0x1f) != 0)
                nextOffset = baseKeyOffset + (nextOffset & 0x00ffffff);
        } else
            nextOffset = this.keys.capacity();
        final int suffixLen = nextOffset - suffixOffset;

        // Fetch the key in two parts, prefix then suffix
        final byte[] key = new byte[prefixLen + suffixLen];
        if (prefixLen > 0) {
            this.keys.position(baseKeyOffset);
            this.keys.get(key, 0, prefixLen);
        }
        assert suffixLen > 0;
        this.keys.position(suffixOffset);
        this.keys.get(key, prefixLen, suffixLen);
        return key;
    }

    /**
     * Read the value at the specified index.
     */
    public byte[] readValue(int index) {
        Preconditions.checkArgument(index >= 0, "index < 0");
        Preconditions.checkArgument(index < this.size, "index >= size");
        final int dataOffset = this.indx.getInt(index * 8 + 4);
        final int nextOffset = (index + 1) < this.size ? this.indx.getInt((index + 1) * 8 + 4) : this.vals.capacity();
        final int length = nextOffset - dataOffset;
        final byte[] data = new byte[length];
        this.vals.position(dataOffset);
        this.vals.get(data);
        return data;
    }

    /**
     * Read the key/value pair at the specified index.
     */
    public KVPair readKV(int index) {
        return new KVPair(this.readKey(index), this.readValue(index));
    }
}

