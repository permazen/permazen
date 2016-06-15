
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jsimpledb.util.ByteUtil;

/**
 * An {@link Iterator} that iterates over all key/value pairs in a {@link KVStore} within a range of keys,
 * without using the {@link KVStore#getRange KVStore.getRange()} method. Therefore, it can be used to implement
 * {@link KVStore#getRange KVStore.getRange()} in {@link KVStore} implementations that don't natively support iteration.
 * Instances support forward or reverse iteration and {@link #remove Iterator.remove()}.
 *
 * <p>
 * The iteration is instead implemented using {@link KVStore#getAtLeast KVStore.getAtLeast()},
 * {@link KVStore#getAtMost KVStore.getAtMost()}, and {@link KVStore#remove KVStore.remove()}.
 *
 * <p><b>Repositioning</b></p>
 *
 * <p>
 * Instances support arbitrary repositioning via {@link #setNextTarget setNextTarget()}.
 *
 * <p><b>Key Restrictions</b></p>
 *
 * <p>
 * Instances are configured with an (optional) {@link KeyRange} that restricts the iteration to the specified key range.
 *
 * <p>
 * Instances also support filtering visible values using a {@link KeyFilter}.
 * To appear in the iteration, keys must both be in the {@link KeyRange} and pass the {@link KeyFilter}, if any.
 *
 * <p><b>Concurrent Modification</b></p>
 *
 * <p>
 * Instances are thread safe, and always reflect the current state of the underlying {@link KVStore},
 * even if it is mutated concurrently.
 */
public class KVPairIterator implements Iterator<KVPair> {

    private final KVStore kv;
    private final boolean reverse;
    private final KeyRange keyRange;
    private final KeyFilter keyFilter;

    private KVPair currPair;            // cached value to return from next()
    private byte[] nextKey;             // next key lower/upper bound to go fetch, or null to start at the beginning
    private byte[] removeKey;           // next key to remove if remove() invoked
    private boolean finished;

// Constructors

    /**
     * Convenience constructor for forward iteration over a specified range. Equivalent to:
     *  <blockquote><code>
     *  KVPairIterator(kv, keyRange, null, false)
     *  </code></blockquote>
     *
     * @param kv underlying {@link KVStore}
     * @param keyRange range restriction on visible keys, or null for none
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public KVPairIterator(KVStore kv, KeyRange keyRange) {
        this(kv, keyRange, null, false);
    }

    /**
     * Convenience constructor for forward iteration over all keys having a given prefix. Equivalent to:
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
     * Convenience constructor for iteration over all keys having a given prefix. Equivalent to:
     *  <blockquote><code>
     *  KVPairIterator(kv, KeyRange.forPrefix(prefix), null, reverse)
     *  </code></blockquote>
     *
     * @param kv underlying {@link KVStore}
     * @param prefix range prefix
     * @param reverse true to iterate in a reverse direction, false to iterate in a forward direction
     * @throws IllegalArgumentException if any parameter is null
     */
    public KVPairIterator(KVStore kv, byte[] prefix, boolean reverse) {
        this(kv, KeyRange.forPrefix(prefix), null, reverse);
    }

    /**
     * Primary constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param keyRange range restriction on visible keys, or null for none
     * @param keyFilter filter restriction on visible keys, or null for none
     * @param reverse true to iterate in a reverse direction, false to iterate in a forward direction
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public KVPairIterator(KVStore kv, KeyRange keyRange, KeyFilter keyFilter, boolean reverse) {
        Preconditions.checkArgument(kv != null, "null kv");
        this.kv = kv;
        this.keyRange = keyRange;
        this.keyFilter = keyFilter;
        this.reverse = reverse;
        this.setNextTarget(null);
    }

// Methods

    /**
     * Get the {@link KVStore} associated with this instance.
     *
     * @return this instance's transaction
     */
    public KVStore getKVStore() {
        return this.kv;
    }

    /**
     * Get the {@link KeyRange} instance used to restrict the range of visible keys, if any.
     *
     * @return {@link KeyRange} over which this iterator iterates, or null if it iterates over all keys
     */
    public KeyRange getKeyRange() {
        return this.keyRange;
    }

    /**
     * Get the {@link KeyFilter} instance used to filter visible keys, if any.
     *
     * @return {@link KeyFilter} in which all keys returned by this iterator must be contained, or null if keys are not filtered
     */
    public KeyFilter getKeyFilter() {
        return this.keyFilter;
    }

    /**
     * Determine if this instance is going forward or backward.
     *
     * @return true if this instance is reversed
     */
    public boolean isReverse() {
        return this.reverse;
    }

    /**
     * Determine if the given key would be visible in this instance. Tests the key against
     * the configured {@link KeyRange} and/or {@link KeyFilter}, if any.
     *
     * @param key to test
     * @return true if key is both in range and not filtered out
     * @throws IllegalArgumentException if {@code key} is null
     */
    public boolean isVisible(byte[] key) {
        Preconditions.checkArgument(kv != null, "null kv");
        return (this.keyRange == null || this.keyRange.contains(key))
          && (this.keyFilter == null || this.keyFilter.contains(key));
    }

    /**
     * Reposition this instance by setting the next "target" key.
     *
     * <p>
     * The target key is the key we will use to find the next element via {@link KVStore#getAtLeast KVStore.getAtLeast()}
     * or {@link KVStore#getAtMost KVStore.getAtMost()} if this is a reverse iterator. In the forward case, the target key is an
     * inclusive lower bound on the next key, while in the reverse case it is an exclusive upper bound on the next key.
     *
     * <p>
     * This method may be used to reposition an interator during iteration or restart an iterator that has been exhausted.
     * Invoking this method does not affect the behavior of {@link #remove}, i.e., you can still {@link #remove} the previously
     * returned element even if you have invoked this method since invoking {@link #next}.
     *
     * <p>
     * A null {@code targetKey} means to reposition this instance at the beginning of the iteration.
     *
     * <p>
     * This instance's configured {@link KeyRange} and {@link KeyFilter}, if any, still apply: if {@code targetKey} is not
     * {@link #isVisible visible} to this instance, the next visible key after {@code targetKey} will be next in the iteration.
     *
     * @param targetKey next lower bound (exclusive) if going forward, or upper bound (exclusive) if going backward;
     *  or null to restart this instance at the beginning of its iteration
     */
    public void setNextTarget(byte[] targetKey) {

        // Clone target key to avoid mutation
        if (targetKey != null)
            targetKey = targetKey.clone();

        // Ensure target is not prior to the beginning of the range
        if (this.keyRange != null) {
            if (this.reverse) {
                final byte[] maxKey = this.keyRange.getMax();
                if (maxKey != null && (targetKey == null || ByteUtil.compare(targetKey, maxKey) > 0))
                    targetKey = maxKey;
            } else {
                final byte[] minKey = this.keyRange.getMin();
                if (targetKey == null || ByteUtil.compare(targetKey, minKey) < 0)
                    targetKey = minKey;
            }
        }

        // Update state
        synchronized (this) {
            this.nextKey = targetKey;
            this.finished = false;
            this.currPair = null;
        }
    }

// Iterator

    @Override
    public synchronized boolean hasNext() {

        // Already have next element?
        if (this.currPair != null)
            return true;

        // No more elements?
        if (this.finished)
            return false;

        // Find next element that is not filtered out by KeyRange
        KVPair pair;
        while (true) {

            // Find next key/value pair
            if ((pair = this.reverse ? this.kv.getAtMost(this.nextKey) : this.kv.getAtLeast(this.nextKey)) == null) {
                this.finished = true;
                return false;
            }
            final byte[] key = pair.getKey();

            // Check key range
            if (this.keyRange != null && !this.keyRange.contains(key)) {
                this.finished = true;
                return false;
            }

            // Check key filter; if going forward, avoid redundant call to seekHigher()
            if (this.keyFilter == null)
                break;
            if (!this.reverse) {
                final byte[] nextHigher = this.keyFilter.seekHigher(key);
                if (nextHigher != null && Arrays.equals(nextHigher, key))
                    break;
                this.nextKey = nextHigher;
            } else {
                if (this.keyFilter.contains(key))
                    break;
                this.nextKey = key.length > 0 ? this.keyFilter.seekLower(key) : null;
            }

            // We have skipped over the filtered-out key range, so try again if there is any left
            if (this.nextKey == null) {
                this.finished = true;
                return false;
            }
        }

        // Save it (pre-fetch)
        this.currPair = pair;
        return true;
    }

    @Override
    public synchronized KVPair next() {

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
        final byte[] removeKeyCopy;
        synchronized (this) {
            if ((removeKeyCopy = this.removeKey) == null)
                throw new IllegalStateException();
            this.removeKey = null;
        }
        this.kv.remove(removeKeyCopy);
    }
}

