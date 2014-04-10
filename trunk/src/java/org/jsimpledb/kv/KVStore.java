
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

/**
 * General API into a key/value store where the keys are sorted lexicographically as unsigned bytes.
 *
 * <p>
 * For some implementations, accessing keys that start with {@code 0xff} is not supported and doing so
 * will result in {@link IllegalArgumentException}.
 * </p>
 */
public interface KVStore {

    /**
     * Get the value associated with the given key, if any.
     *
     * @param key key
     * @return value associated with key, or null if not found
     * @throws IllegalArgumentException if {@code key} starts with {@code 0xff} and such keys are not supported
     * @throws NullPointerException if {@code key} is null
     */
    byte[] get(byte[] key);

    /**
     * Get the key/value pair having the smallest key greater than or equal to the given minimum, if any.
     *
     * @param minKey minimum key (inclusive), or null for no minimum (get the smallest key)
     * @return smallest key/value pair with {@code key >= minKey}, or null if none exists
     */
    KVPair getAtLeast(byte[] minKey);

    /**
     * Get the key/value pair having the largest key strictly less than the given maximum, if any.
     *
     * @param maxKey maximum key (exclusive), or null for no maximum (get the largest key)
     * @return largest key/value pair with {@code key < maxKey}, or null if none exists
     */
    KVPair getAtMost(byte[] maxKey);

    /**
     * Set the value associated with the given key.
     *
     * @param key key
     * @param value value
     * @throws IllegalArgumentException if {@code key} starts with {@code 0xff} and such keys are not supported
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws RetryTransactionException if this transaction must be retried and is no longer usable
     * @throws NullPointerException if {@code key} or {@code value} is null
     */
    void put(byte[] key, byte[] value);

    /**
     * Remove the key/value pair with the given key, if it exists.
     *
     * @param key key
     * @throws IllegalArgumentException if {@code key} starts with {@code 0xff} and such keys are not supported
     * @throws NullPointerException if {@code key} is null
     */
    void remove(byte[] key);

    /**
     * Remove all key/value pairs whose keys are in a given range.
     *
     * <p>
     * The {@code minKey} must be less than or equal to {@code maxKey}; if they equal (and not null)
     * then nothing happens; if they are both null then all entries are deleted.
     * </p>
     *
     * @param minKey minimum key (inclusive), or null for no minimum
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code minKey > maxKey}
     */
    void removeRange(byte[] minKey, byte[] maxKey);
}

