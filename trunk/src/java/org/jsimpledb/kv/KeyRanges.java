
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import java.util.Arrays;
import java.util.Collections;

/**
 * Zero or more {@link KeyRange}s used together as a group to form a boolean predicate for {@code byte[]} keys.
 * Keys are {@linkplain #contains contained} in a {@link KeyRanges} instance if there exists an underlying
 * {@link KeyRange} containing the key. However, the underlying {@link KeyRange}s are not necessarily directly
 * accessible; for example, they could constitute an infinite and/or recursively defined set.
 *
 * <p>
 * The underlying {@link KeyRange}s must be non-empty, non-overlapping, and separated (i.e., not share a boundary).
 * </p>
 *
 * <p>
 * {@link KeyRanges} instances are "navigable" in the sense that they are capable of efficiently locating the next
 * higher or lower {@link KeyRange} starting from a given {@code byte[]} key that's not contained.
 * Instances assume keys are ordered in unsigned lexicographical order.
 * </p>
 *
 * @see SimpleKeyRanges
 */
public interface KeyRanges {

    /**
     * The empty instance containing zero ranges.
     */
    SimpleKeyRanges EMPTY = new SimpleKeyRanges(Collections.<KeyRange>emptyList());

    /**
     * The "full" instance containing a single {@link KeyRange} that contains all keys.
     */
    SimpleKeyRanges FULL = new SimpleKeyRanges(Arrays.asList(KeyRange.FULL));

    /**
     * Determine whether this instance contains the given key.
     *
     * @param key key to test
     * @return true if {@code key} is contained by this instance, otherwise false
     * @throws IllegalArgumentException if {@code key} is null
     */
    boolean contains(byte[] key);

    /**
     * Create the inverse of this instance. The inverse contains all keys not contained by this instance.
     *
     * @return the inverse of this instance
     */
    KeyRanges inverse();

    /**
     * Find the lowest {@link KeyRange} in this instance whose upper bound is strictly greater than {@code key}.
     * If {@code key} is {@linkplain #contains contained} by this instance, return the {@link KeyRange} that contains it.
     *
     * @param key
     * @return the {@link KeyRange} in this instance containing {@code key}, if any, otherwise the next {@link KeyRange}
     *  after {@code key}, if any, otherwise null
     * @throws IllegalArgumentException if {@code key} is null
     */
    KeyRange nextHigherRange(byte[] key);

    /**
     * Find the highest {@link KeyRange} in this instance whose lower bound is greater than or equal to {@code key}.
     * If {@code key} is {@linkplain #contains contained} by this instance, return the {@link KeyRange} that contains it.
     *
     * @param key
     * @return the {@link KeyRange} in this instance containing {@code key}, if any, otherwise the next {@link KeyRange}
     *  before {@code key}, if any, otherwise null
     * @throws IllegalArgumentException if {@code key} is null
     */
    KeyRange nextLowerRange(byte[] key);
}
