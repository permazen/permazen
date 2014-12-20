
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

/**
 * A predicate for filtering {@code byte[]} keys that is also capable of efficiently jumping over
 * contiguous regions of uncontained keys.
 *
 * <p>
 * Instances assume {@code byte[]} keys are ordered in unsigned lexicographical order.
 * </p>
 */
public interface KeyFilter {

    /**
     * Determine whether this instance contains the given key.
     *
     * @param key key to test
     * @return true if {@code key} is contained by this instance, otherwise false
     * @throws IllegalArgumentException if {@code key} is null
     */
    boolean contains(byte[] key);

    /**
     * Skip over the largest possible uncontained region in an upward direction.
     *
     * <p>
     * This method should return an inclusive lower bound on all keys greater than or equal to {@code key}
     * that are contained by this instance. The bound does not have to be tight, but the tighter the better.
     * </p>
     *
     * <p>
     * A value of null may be returned to indicate that no key greater than or equal to {@code key} is contained by this instance.
     * </p>
     *
     * <p>
     * If {@code key} is not {@linkplain #contains contained by} this instance, this method may either return a valid
     * value as described above or throw an {@link UnsupportedOperationException} if seeking upward from {@link key}
     * is not supported.
     * </p>
     *
     * <p>
     * If {@code key} is contained by this instance, this method must return {@code key};
     * if {@code key} is not contained by this instance, this method must return a key strictly higher than {@code key} or null.
     * </p>
     *
     * @param key starting key
     * @return a lower bound (inclusive) for contained keys greater than or equal to {@code key},
     *  or null if no key greater than or equal to {@code key} is contained by this instance
     * @throws UnsupportedOperationException if {@code key} is not contained by this instance
     *  and seeking upward from {@code key} is not supported
     * @throws IllegalArgumentException if {@code key} is null
     */
    byte[] seekHigher(byte[] key);

    /**
     * Skip over the largest possible uncontained region in a downward direction.
     *
     * <p>
     * This method should return an exclusive upper bound on all keys strictly less than {@code key}
     * that are contained by this instance. The bound does not have to be tight, but the tighter the better.
     * </p>
     *
     * <p>
     * A value of null may be returned to indicate that no key strictly less than {@code key} is contained by this instance.
     * </p>
     *
     * <p>
     * If {@code key} is not {@linkplain #contains contained by} this instance, this method may either return a valid
     * value as described above or throw an {@link UnsupportedOperationException} if seeking downward from {@link key}
     * is not supported.
     * </p>
     *
     * @param key starting key
     * @return an upper bound (exclusive) for contained keys strictly less that {@code key},
     *  or null if no key strictly less than {@code key} is contained by this instance
     * @throws UnsupportedOperationException if {@code key} is not contained by this instance
     *  and seeking downward from {@code key} is not supported
     * @throws IllegalArgumentException if {@code key} is null
     */
    byte[] seekLower(byte[] key);
}

