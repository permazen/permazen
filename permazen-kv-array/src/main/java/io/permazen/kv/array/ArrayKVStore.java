
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * A simple read-only {@link KVStore} based on a sorted array of key/value pairs.
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
        this.size = indx.capacity() / 8;
        this.finder = new ArrayKVFinder(indx, keys, vals);
    }

    @Override
    public ByteData get(ByteData key) {
        final int index = this.finder.find(key);
        if (index < 0)
            return null;
        return this.finder.readValue(index);
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        int index;
        if (minKey == null || minKey.isEmpty())
            index = 0;
        else if ((index = this.finder.find(minKey)) < 0)
            index = ~index;
        if (index == this.size)
            return null;
        final KVPair pair = this.finder.readKV(index);
        assert pair.getKey().compareTo(minKey) >= 0;
        return maxKey == null || pair.getKey().compareTo(maxKey) < 0 ? pair : null;
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        int index;
        if (maxKey == null)
            index = this.size;
        else if ((index = this.finder.find(maxKey)) < 0)
            index = ~index;
        if (index == 0)
            return null;
        final KVPair pair = this.finder.readKV(index - 1);
        assert pair.getKey().compareTo(maxKey) < 0;
        return minKey == null || pair.getKey().compareTo(minKey) >= 0 ? pair : null;
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, final boolean reverse) {

        // Find min index
        int index;
        if (minKey == null || minKey.isEmpty())
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
        return new RangeIter(minIndex, maxIndex, reverse);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(ByteData key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        throw new UnsupportedOperationException();
    }

// RangeIter

    private class RangeIter extends UnmodifiableIterator<KVPair> implements CloseableIterator<KVPair> {

        private final int limit;
        private final boolean reverse;

        private int index;

        RangeIter(int minIndex, int maxIndex, boolean reverse) {
            this.index = reverse ? maxIndex : minIndex;
            this.limit = reverse ? minIndex : maxIndex;
            this.reverse = reverse;
        }

        @Override
        public boolean hasNext() {
            return this.reverse ? this.index > this.limit : this.index < this.limit;
        }

        @Override
        public KVPair next() {
            if (!this.hasNext())
                throw new NoSuchElementException();
            return ArrayKVStore.this.finder.readKV(this.reverse ? --this.index : this.index++);
        }

        @Override
        public void close() {
        }
    }
}
