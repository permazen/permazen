
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteUtil;

import java.nio.ByteBuffer;

/**
 * Performs searches into an {@link ArrayKVStore}.
 *
 * <p>
 * Instances are thread safe.
 */
class ArrayKVFinder {

    // Note: for thread safety, perform only absolute gets
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
            return this.get(this.keys, baseKeyOffset, new byte[length], 0, length);
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
        if (prefixLen > 0)
            this.get(this.keys, baseKeyOffset, key, 0, prefixLen);
        assert suffixLen > 0;
        return this.get(this.keys, suffixOffset, key, prefixLen, suffixLen);
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
        return this.get(this.vals, dataOffset, new byte[length], 0, length);
    }

    /**
     * Read the key/value pair at the specified index.
     */
    public KVPair readKV(int index) {
        return new KVPair(this.readKey(index), this.readValue(index));
    }

    // Perform a bulk get() that doesn't modify the buffer
    protected byte[] get(ByteBuffer buf, int position, byte[] dest, int off, int len) {
        if (buf.hasArray())
            System.arraycopy(buf.array(), buf.arrayOffset() + position, dest, off, len);
        else if (len < 128) {                               // 128 is a wild guess TODO: determine through performance testing
            while (len-- > 0)
                dest[off++] = buf.get(position++);
        } else
            buf.duplicate().position(position).get(dest, off, len);
        return dest;
    }
}

