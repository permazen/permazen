
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import org.jsimpledb.kv.KVPair;

/**
 * Performs searches into an {@link ArrayKVStore}, caching search bounds from the previous search
 * to speed up the next search if it has a shared prefix.
 *
 * <p>
 * Instances are not thread safe.
 */
class ArrayKVFinder {

    private static final int MAX_PREFIX_LENGTH = 20;
    private static final int END = -1;

    private final ByteBuffer indx;
    private final ByteBuffer keys;
    private final ByteBuffer vals;
    private final int size;

/*
    Invariant:

        For all 0 < i <= this.prefixLength, all keys with prefix this.prefix[0..i-1] live at some index such that:

            prefixMin[i - 1] <= index < prefixMax[i - 1]

    To maintain this invariant, we apply the following logic to extend the cached search prefix.
    At each comparison between the next search key byte and the next entry key byte, there are six cases:

        1. The two bytes match.

            We cannot infer anything new about the set of keys that have the new prefix byte over what we
            know about the set of keys without the new prefix byte. Continue.

        2. Search key byte < entry key byte.

            Then search key < entry key, as are all keys with the prefix that includes the new search key byte.
            We add the search key byte, copy the previous lower bound, and set the entry key index as the upper bound.

        3. Search key byte > entry key byte.

            Then search key > entry key, as are all keys with the prefix that includes the new search key byte.
            We add the search key byte, copy the previous upper bound, and set the entry key index + 1 as the lower bound.

        4. Both the search key and the entry key have no more bytes.

            They are equal. We cannot infer anything new and do not increase the cached prefix length.

        5. The search key has no more bytes, but the entry has more bytes.

            Then search key < entry key. We don't infer anything new (although we could).

        6. The search key has more bytes, but the entry has no more bytes.

            Then search key > entry key, as are all keys with the prefix that includes the new search key byte.
            We add the search key byte, copy the previous upper bound, and set the entry key index + 1 as the lower bound.

*/

    // Cached search prefix bounds
    private final byte[] prefix = new byte[MAX_PREFIX_LENGTH];      // prefix bytes (only this.prefixLength of them are valid)
    private final int[] prefixMin = new int[MAX_PREFIX_LENGTH];     // lower bound for keys having prefix[0..i-1]
    private final int[] prefixMax = new int[MAX_PREFIX_LENGTH];     // upper bound for keys having prefix[0..i-1]
    private int prefixLength;

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
        for (int i = 0; i < this.prefixLength; i++) {
            final int diff = i < searchKey.length ? (searchKey[i] & 0xff) - (this.prefix[i] & 0xff) : -1;
            if (diff <= 0)
                max = Math.min(max, this.prefixMax[i]);
            if (diff >= 0)
                min = Math.max(min, this.prefixMin[i]);
            if (diff != 0) {
                this.prefixLength = i;
                break;
            }
        }

        // Perform binary search for key, starting at the point where we diverged from the previous search key
        while (min < max) {

            // Calculate the midpoint of the search range
            final int mid = (min + (max - 1)) >>> 1;

            // Get key at midpoint
            final byte[] midKey = this.readKey(mid);

            // Compare search key to the midpoint entry key
            int len = 0;
            boolean extendPrefix = false;
            boolean newLowerPrefixBound = false;
            boolean newUpperPrefixBound = false;
            while (true) {

                // Check if either key has been exhausted
                if (len == searchKey.length) {
                    if (len == midKey.length)
                        return mid;
                    max = mid;
                    break;
                } else if (len == midKey.length) {
                    min = mid + 1;
                    extendPrefix = true;
                    newLowerPrefixBound = true;
                    break;
                }

                // Compare the next byte
                final int searchKeyByte = searchKey[len] & 0xff;
                final int midKeyByte = midKey[len] & 0xff;
                if (searchKeyByte < midKeyByte) {
                    max = mid;
                    extendPrefix = true;
                    newUpperPrefixBound = true;
                    break;
                } else if (searchKeyByte > midKeyByte) {
                    min = mid + 1;
                    extendPrefix = true;
                    newLowerPrefixBound = true;
                    break;
                }

                // Advance to next byte
                len++;
            }

            // Update prefix bounds after previous byte match
            if (extendPrefix) {
                while (this.prefixLength < len && this.prefixLength < MAX_PREFIX_LENGTH) {
                    final int next = this.prefixLength;
                    this.prefix[next] = searchKey[next];
                    this.prefixMin[next] = next > 0 ? this.prefixMin[next - 1] : 0;
                    this.prefixMax[next] = next > 0 ? this.prefixMax[next - 1] : this.size;
                    this.prefixLength++;
                }
                if (this.prefixLength < MAX_PREFIX_LENGTH) {
                    this.prefix[len] = searchKey[len];
                    int nextMin = len > 0 ? this.prefixMin[len - 1] : 0;
                    int nextMax = len > 0 ? this.prefixMax[len - 1] : this.size;
                    if (newLowerPrefixBound)
                        nextMin = Math.max(nextMin, min);
                    if (newUpperPrefixBound)
                        nextMax = Math.min(nextMax, max);
                    this.prefixMin[len] = nextMin;
                    this.prefixMax[len] = nextMax;
                    this.prefixLength = len + 1;
                }
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

