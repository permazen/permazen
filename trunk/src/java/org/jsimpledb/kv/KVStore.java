
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import java.util.Iterator;

/**
 * General API into a key/value store where the keys are sorted lexicographically as unsigned bytes.
 *
 * <p>
 * Implementations of this interface are not required to support accessing keys that start with {@code 0xff}.
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
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, and {@code minKey} starts with {@code 0xff},
     * then this method returns null.
     * </p>
     *
     * @param minKey minimum key (inclusive), or null for no minimum (get the smallest key)
     * @return smallest key/value pair with {@code key >= minKey}, or null if none exists
     */
    KVPair getAtLeast(byte[] minKey);

    /**
     * Get the key/value pair having the largest key strictly less than the given maximum, if any.
     *
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, and {@code maxKey} starts with {@code 0xff},
     * then this method behaves as if {@code maxKey} were null.
     * </p>
     *
     * @param maxKey maximum key (exclusive), or null for no maximum (get the largest key)
     * @return largest key/value pair with {@code key < maxKey}, or null if none exists
     */
    KVPair getAtMost(byte[] maxKey);

    /**
     * Iterate the key/value pairs in the specified range. The returned {@link Iterator}'s {@link Iterator#remove remove()}
     * must be supported and should have the same effect as invoking {@link #remove remove()} on the corresponding key.
     *
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, and {@code minKey} starts with {@code 0xff},
     * then this method returns an empty iteration.
     * </p>
     *
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, and {@code maxKey} starts with {@code 0xff},
     * then this method behaves as if {@code maxKey} were null.
     * </p>
     *
     * <p>
     * The returned {@link Iterator} must not throw {@link java.util.ConcurrentModificationException};
     * however, it is undefined whether or not the {@link Iterator} reflects any modifications made after its creation.
     * </p>
     *
     * @param minKey minimum key (inclusive), or null for no minimum (start at the smallest key)
     * @param maxKey maximum key (exclusive), or null for no maximum (end at the largest key)
     * @param reverse true to return key/value pairs in reverse order (i.e., keys descending)
     * @return iteration of key/value pairs in the range {@code minKey} (inclusive) to {@code maxKey} (exclusive)
     */
    Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse);

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
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, then:
     * <ul>
     *  <li>If {@code minKey} starts with {@code 0xff}, then no change occurs</li>
     *  <li>If {@code maxKey} starts with {@code 0xff}, then this method behaves as if {@code maxKey} were null</li>
     * </ul>
     * </p>
     *
     * @param minKey minimum key (inclusive), or null for no minimum
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code minKey > maxKey}
     */
    void removeRange(byte[] minKey, byte[] maxKey);
}

