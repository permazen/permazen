
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
 * An {@link Iterator} that iterates over all key/value pairs in a {@link KVStore} within a range of keys,
 * without using the {@link KVStore#getRange KVStore.getRange()} method. Therefore, it can be used to implement
 * {@link KVStore#getRange KVStore.getRange()} in {@link KVStore} implementations that don't natively support iteration.
 *
 * <p>
 * The iteration is instead implemented using {@link KVStore#getAtLeast KVStore.getAtLeast()},
 * {@link KVStore#getAtMost KVStore.getAtMost()}, and {@link KVStore#remove KVStore.remove()}.
 * Instances support {@linkplain #setNextTarget repositioning}, forward or reverse iteration, and {@link #remove removal}.
 * </p>
 *
 * <p>
 * Instances support restricting the keys in the iteration to those contained in a {@link KeyRanges} instance.
 * This filtering is implemented efficiently: skipping over an interval of invisible keys requires only one
 * {@link KVStore#getAtLeast KVStore.getAtLeast()} or {@link KVStore#getAtMost KVStore.getAtMost()} call.
 * </p>
 */
public class KVPairIterator implements Iterator<KVPair> {

    protected final KVStore kv;
    protected final boolean reverse;

    private final KeyRanges keyRanges;

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
        this(kv, new KeyRanges(minKey, maxKey), reverse);
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
        this(kv, KeyRanges.forPrefix(prefix), reverse);
    }

    /**
     * Primary constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param keyRanges restriction on visible keys, or null for none
     * @param reverse true to iterate in a reverse direction, false to iterate in a forward direction
     * @throws IllegalArgumentException if any parameter is null
     */
    public KVPairIterator(KVStore kv, KeyRanges keyRanges, boolean reverse) {
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        this.kv = kv;
        this.keyRanges = keyRanges;
        this.reverse = reverse;
        this.nextKey = keyRanges != null ? (reverse ? this.keyRanges.getMax() : this.keyRanges.getMin()) : null;
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
     * Get the {@link KeyRanges} instance used to restrict visible keys, if any.
     *
     * @return {@link KeyRanges} in which all keys returned by this iterator must be contained, or null if keys are unrestricted
     */
    public KeyRanges getKeyRanges() {
        return this.keyRanges;
    }

    /**
     * Determine if this instance is going forward or backward.
     */
    public boolean isReverse() {
        return this.reverse;
    }

    /**
     * Determine if the given key is visible in this instance. Equivalent to:
     *  <blockquote><code>
     *  this.getKeyRanges() == null || this.getKeyRanges().contains(key)
     *  </code></blockquote>
     *
     * @return true if key is greater than or equal to this instance's minimum key (if an)
     *  and strictly less than this instance's maximum key (if any)
     * @throws IllegalArgumentException if {@code key} is null
     */
    public boolean isVisible(byte[] key) {
        if (key == null)
            throw new IllegalArgumentException("null key");
        return this.keyRanges == null || this.keyRanges.contains(key);
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
     * <p>
     * This instance's configured {@link KeyRanges} instance, if any, still applies: if {@code targetKey} is not
     * {@link #isVisible visible} to this instance, the next visible key after (or before) {@code targetKey} will be used instead.
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

        // Find next element that is not filtered out by KeyRanges
        KVPair pair;
        while (true) {

            // Find next element
            pair = this.reverse ?
              this.kv.getAtMost(this.nextKey) : this.kv.getAtLeast(this.nextKey != null ? this.nextKey : ByteUtil.EMPTY);
            if (pair == null) {
                this.finished = true;
                return false;
            }

            // Check if key is visible
            if (this.isVisible(pair.getKey()))
                break;

            // Skip ahead to the next visible key range
            assert this.keyRanges != null;
            final KeyRange keyRange = this.keyRanges.getKeyRange(pair.getKey(), !this.reverse);
            if (keyRange == null) {
                this.finished = true;
                return false;
            }

            // Try again
            this.nextKey = this.reverse ? keyRange.getMax() : keyRange.getMin();
        }

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
}

