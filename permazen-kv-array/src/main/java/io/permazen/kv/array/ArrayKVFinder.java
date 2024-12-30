
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteData;

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
    public int find(ByteData searchKey) {

        // Initialize bounds using cached prefix
        int min = 0;
        int max = this.size;

        // Perform binary search for key, starting at the point where we diverged from the previous search key
        ByteData prevMin = null;
        ByteData prevMax = null;
        while (min < max) {

            // Calculate the midpoint of the search range
            final int mid = (min + (max - 1)) >>> 1;

            // Get key at midpoint
            final ByteData midKey = this.readKey(mid);
            assert prevMin == null || searchKey.compareTo(prevMin) > 0;
            assert prevMax == null || searchKey.compareTo(prevMax) < 0;

            // Compare search key to the midpoint key
            final int diff = searchKey.compareTo(midKey);
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
    public ByteData readKey(int index) {

        // Sanity check
        Preconditions.checkArgument(index >= 0, "index < 0");
        Preconditions.checkArgument(index < this.size, "index >= size");

        // If this is a base key, read absolute offset and fetch data normally
        final int baseIndex = index & ~0x1f;
        final int baseKeyOffset = this.indx.getInt(baseIndex * 8);
        if (index == baseIndex) {
            final int length = (index + 1) < this.size ?
              this.indx.getInt((index + 1) * 8) & 0x00ffffff : this.keys.capacity() - baseKeyOffset;
            final ByteData.Writer writer = ByteData.newWriter(length);
            this.writeTo(writer, this.keys, baseKeyOffset, length);
            return writer.toByteData();
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
        final ByteData.Writer writer = ByteData.newWriter(prefixLen + suffixLen);
        if (prefixLen > 0)
            this.writeTo(writer, this.keys, baseKeyOffset, prefixLen);
        assert suffixLen > 0;
        this.writeTo(writer, this.keys, suffixOffset, suffixLen);
        return writer.toByteData();
    }

    /**
     * Read the value at the specified index.
     */
    public ByteData readValue(int index) {
        Preconditions.checkArgument(index >= 0, "index < 0");
        Preconditions.checkArgument(index < this.size, "index >= size");
        final int dataOffset = this.indx.getInt(index * 8 + 4);
        final int nextOffset = (index + 1) < this.size ? this.indx.getInt((index + 1) * 8 + 4) : this.vals.capacity();
        final int length = nextOffset - dataOffset;
        final ByteData.Writer writer = ByteData.newWriter(length);
        this.writeTo(writer, this.vals, dataOffset, length);
        return writer.toByteData();
    }

    /**
     * Read the key/value pair at the specified index.
     */
    public KVPair readKV(int index) {
        return new KVPair(this.readKey(index), this.readValue(index));
    }

    // Perform a bulk get() that doesn't modify the buffer
    protected void writeTo(ByteData.Writer writer, ByteBuffer buf, int position, int len) {
        if (buf.hasArray()) {
            writer.write(buf.array(), buf.arrayOffset() + position, len);
            return;
        }
        buf = buf.duplicate().position(position);
        final byte[] xferBuf = new byte[Math.min(len, 1000)];
        while (len > 0) {
            final int xferLen = Math.min(len, xferBuf.length);
            buf.get(xferBuf, 0, xferLen);
            writer.write(xferBuf, 0, xferLen);
            len -= xferLen;
        }
    }
}
