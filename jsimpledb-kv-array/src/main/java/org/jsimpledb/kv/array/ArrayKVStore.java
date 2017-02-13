
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.util.ByteUtil;

/**
 * A simple read-only {@link org.jsimpledb.kv.KVStore} based on a sorted array of key/value pairs.
 *
 * <p>
 * Instances query three {@link ByteBuffer}s, one for the array index, one for the key data, and one for the value data.
 * Data for these {@link ByteBuffer}s is created using {@link ArrayKVWriter}.
 *
 * <p>
 * Instances are optimized for minimal memory overhead and queries using keys sharing a prefix with the previously
 * queried key. Key data is prefix-compressed.
 *
 * <p>
 * Key and value data must not exceed 2GB (each separately).
 */
public class ArrayKVStore extends AbstractKVStore {

    private final ByteBuffer indx;
    private final ByteBuffer keys;
    private final ByteBuffer vals;
    private final int size;
    private final ArrayKVFinder finder;

    /**
     * Constructor.
     *
     * @param indx buffer containing index data written by a {@link ArrayKVWriter}
     * @param keys buffer containing key data written by a {@link ArrayKVWriter}
     * @param vals buffer containing value data written by a {@link ArrayKVWriter}
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code indx} size is not a correct multiple
     */
    public ArrayKVStore(ByteBuffer indx, ByteBuffer keys, ByteBuffer vals) {
        Preconditions.checkArgument(indx != null, "null indx");
        Preconditions.checkArgument(keys != null, "null keys");
        Preconditions.checkArgument(vals != null, "null vals");
        Preconditions.checkArgument(indx.capacity() % 8 == 0, "index size is not a multiple of 8");
        this.indx = indx;
        this.keys = keys;
        this.vals = vals;
        this.size = this.indx.capacity() / 8;
        this.finder = new ArrayKVFinder(this.indx, this.keys, this.vals);
    }

    @Override
    public byte[] get(byte[] key) {
        final int index = this.finder.find(key);
        if (index < 0)
            return null;
        return this.finder.readValue(index);
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        int index;
        if (minKey == null || minKey.length == 0)
            index = 0;
        else if ((index = this.finder.find(minKey)) < 0)
            index = ~index;
        if (index == this.size)
            return null;
        final KVPair pair = this.finder.readKV(index);
        assert ByteUtil.compare(pair.getKey(), minKey) >= 0;
        return pair;
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        int index;
        if (maxKey == null)
            index = this.size;
        else if ((index = this.finder.find(maxKey)) < 0)
            index = ~index;
        if (index == 0)
            return null;
        final KVPair pair = this.finder.readKV(index - 1);
        assert ByteUtil.compare(pair.getKey(), maxKey) < 0;
        return pair;
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, final boolean reverse) {

        // Find min index
        int index;
        if (minKey == null || minKey.length == 0)
            index = 0;
        else if ((index = this.finder.find(minKey)) < 0)
            index = ~index;
        final int minIndex = index;

        // Find max index
        if (maxKey == null)
            index = this.size;
        else if ((index = this.finder.find(maxKey)) < 0)
            index = ~index;
        final int maxIndex = index;

        // Return iterator over array indexes
        return new UnmodifiableIterator<KVPair>() {

            private int index = reverse ? maxIndex : minIndex;

            @Override
            public boolean hasNext() {
                return reverse ? this.index > minIndex : this.index < maxIndex;
            }

            @Override
            public KVPair next() {
                if (!this.hasNext())
                    throw new NoSuchElementException();
                return ArrayKVStore.this.finder.readKV(reverse ? --this.index : this.index++);
            }
        };
    }

    @Override
    public void put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        throw new UnsupportedOperationException();
    }
}

