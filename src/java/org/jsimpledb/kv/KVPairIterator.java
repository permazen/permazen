
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jsimpledb.util.ByteUtil;

/**
 * An {@link Iterator} that iterates over all key/value pairs in a {@link KVStore} within a contiguous range of keys,
 * without using the {@link KVStore#getRange KVStore.getRange()} method. Therefore, it can be used to implement
 * {@link KVStore#getRange KVStore.getRange()} in {@link KVStore} implementations that don't natively support iteration.
 *
 * <p>
 * The iteration is instead implemented using {@link KVStore#getAtLeast KVStore.getAtLeast()},
 * {@link KVStore#getAtMost KVStore.getAtMost()}, and {@link KVStore#remove KVStore.remove()}.
 * Instances support {@linkplain #setNextTarget repositioning}, forward or reverse iteration, and {@link #remove removal}.
 * </p>
 */
public class KVPairIterator implements Iterator<KVPair> {

    protected final KVStore kv;
    protected final boolean reverse;

    private final byte[] prefix;
    private final byte[] minKey;
    private final byte[] maxKey;

    private KVPair currPair;            // cached value to return from next()
    private byte[] nextKey;             // next key lower/upper bound to go fetch, or null for none
    private byte[] removeKey;           // next key to remove if remove() invoked
    private boolean finished;

    /**
     * Convenience constructor for forward iteration and arbitrary minimum and maximum keys. Equivalent to:
     *  <blockquote><code>
     *  KVPairIterator(kv, minKey, maxKey, true)
     *  </code></blockquote>
     *
     * @param kv underlying {@link KVStore}
     * @param minKey minimum key (inclusive), or null for no minimum
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code minKey > maxKey}
     */
    public KVPairIterator(KVStore kv, byte[] minKey, byte[] maxKey) {
        this(kv, minKey, maxKey, true);
    }

    /**
     * Convenience constructor for forward iteration when the range is defined as all keys having a given prefix. Equivalent to:
     *  <blockquote><code>
     *  KVPairIterator(kv, prefix, false)
     *  </code></blockquote>
     *
     * @param kv underlying {@link KVStore}
     * @param prefix range prefix
     * @throws IllegalArgumentException if any parameter is null
     */
    public KVPairIterator(KVStore kv, byte[] prefix) {
        this(kv, prefix, false);
    }

    /**
     * Primary constructor handling arbitrary minimum and maximum keys.
     *
     * @param kv underlying {@link KVStore}
     * @param minKey minimum key (inclusive), or null for no minimum
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @param reverse true to iterate in a reverse direction, false to iterate in a forward direction
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code minKey > maxKey}
     */
    public KVPairIterator(KVStore kv, byte[] minKey, byte[] maxKey, boolean reverse) {
        this(kv, minKey, maxKey, null, reverse);
    }

    /**
     * Primary constructor for when the range is defined as all keys having a given prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefix range prefix
     * @param reverse true to iterate in a reverse direction, false to iterate in a forward direction
     * @throws IllegalArgumentException if any parameter is null
     */
    public KVPairIterator(KVStore kv, byte[] prefix, boolean reverse) {
        this(kv, prefix, KVPairIterator.getMaxKey(prefix), prefix, reverse);
    }

    private KVPairIterator(KVStore kv, byte[] minKey, byte[] maxKey, byte[] prefix, boolean reverse) {
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) > 0)
            throw new IllegalArgumentException("minKey > maxKey");
        this.kv = kv;
        this.minKey = minKey != null ? minKey.clone() : null;
        this.maxKey = maxKey != null ? maxKey.clone() : null;
        this.prefix = prefix != null ? prefix.clone() : null;
        this.reverse = reverse;
        this.nextKey = reverse ? this.maxKey : this.minKey;
    }

    /**
     * Get the {@link KVStore} associated with this instance.
     *
     * @return this instance's transaction
     */
    public KVStore getKVStore() {
        return this.kv;
    }

    /**
     * Get the prefix associated with this instance, if any.
     *
     * @return a modifiable copy of this instance's range prefix if a prefix constructor was used, otherwise null
     */
    public byte[] getPrefix() {
        return this.prefix.clone();
    }

    /**
     * Determine if this instance is going forward or backward.
     */
    public boolean isReverse() {
        return this.reverse;
    }

    /**
     * Determine if the given key is within this iterator's original range of keys.
     *
     * @return true if key is greater than or equal to this instance's minimum key (if an)
     *  and strictly less than this instance's maximum key (if any)
     */
    public boolean inRange(byte[] key) {
        return (this.minKey == null || ByteUtil.compare(key, this.minKey) >= 0)
          && (this.maxKey == null || ByteUtil.compare(key, this.maxKey) < 0);
    }

    /**
     * Set the next target key.
     *
     * <p>
     * This is the key we will use to find the next element via {@link KVStore#getAtLeast}
     * (or {@link KVStore#getAtMost} if this is a reverse iterator). Note that in the forward case, this is an
     * inclusive lower bound on the next key, while in the reverse case it is an exclusive upper bound on the next key.
     * </p>
     *
     * @param targetKey next lower bound (exclusive) if going forward, or upper bound (exclusive) if going backward,
     *  or null to immediately end the iteration
     */
    public void setNextTarget(byte[] targetKey) {
        this.nextKey = targetKey.clone();
    }

// Iterator

    @Override
    public boolean hasNext() {

        // Already have next element?
        if (this.currPair != null)
            return true;

        // No more elements?
        if (this.finished)
            return false;

        // Find next element
        final KVPair pair = this.reverse ?
          this.kv.getAtMost(this.nextKey) : this.kv.getAtLeast(this.nextKey != null ? this.nextKey : ByteUtil.EMPTY);
        if (pair == null) {
            this.finished = true;
            return false;
        }

        // Check if within range
        if (!this.inRange(pair.getKey()))
            return false;

        // Save it (pre-fetch)
        this.currPair = pair;
        return true;
    }

    @Override
    public KVPair next() {

        // Check there is a next element
        if (this.currPair == null && !this.hasNext())
            throw new NoSuchElementException();

        // Get next element
        final KVPair pair = this.currPair;
        final byte[] key = pair.getKey().clone();
        this.removeKey = key;

        // Set up next advance
        this.nextKey = this.reverse ? key : ByteUtil.getNextKey(key);
        this.currPair = null;

        // Done
        return pair;
    }

    @Override
    public void remove() {
        if (this.removeKey == null)
            throw new IllegalStateException();
        this.kv.remove(this.removeKey);
        this.removeKey = null;
    }

// Internal methods

    private static byte[] getMaxKey(byte[] prefix) {
        if (prefix == null)
            throw new IllegalArgumentException("null prefix");
        try {
            return ByteUtil.getKeyAfterPrefix(prefix);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}

