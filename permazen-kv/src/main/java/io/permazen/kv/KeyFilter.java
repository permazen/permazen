
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import io.permazen.util.ByteData;

/**
 * A predicate for filtering {@code byte[]} keys that is also capable of efficiently jumping over
 * contiguous regions of uncontained keys.
 *
 * <p>
 * Instances assume {@code byte[]} keys are ordered in unsigned lexicographical order.
 */
public interface KeyFilter {

    /**
     * Determine whether this instance contains the given key.
     *
     * @param key key to test
     * @return true if {@code key} is contained by this instance, otherwise false
     * @throws IllegalArgumentException if {@code key} is null
     */
    boolean contains(ByteData key);

    /**
     * Skip over the largest possible uncontained region in an upward direction.
     *
     * <p>
     * This method should return an inclusive lower bound on all keys greater than or equal to {@code key}
     * that are contained by this instance. The bound does not have to be tight, but the tighter the better.
     *
     * <p>
     * A value of null may be returned to indicate that no key greater than or equal to {@code key} is contained by this instance.
     *
     * <p>
     * If {@code key} is contained by this instance, this method must return {@code key};
     * if {@code key} is not contained by this instance, this method must return a key strictly higher than {@code key} or null.
     *
     * @param key starting key
     * @return a lower bound (inclusive) for contained keys greater than or equal to {@code key},
     *  or null if no key greater than or equal to {@code key} is contained by this instance
     * @throws IllegalArgumentException if {@code key} is null
     */
    ByteData seekHigher(ByteData key);

    /**
     * Skip over the largest possible uncontained region in a downward direction.
     *
     * <p>
     * This method should return an exclusive upper bound on all keys strictly less than {@code key}
     * that are contained by this instance. The bound does not have to be tight, but the tighter the better.
     *
     * <p>
     * A value of null may be returned to indicate that no key strictly less than {@code key} is contained by this instance.
     *
     * <p>
     * For the purposes of this method, an empty {@code byte[]} array represents an upper bound greater than all
     * {@code byte[]} keys. This interpretation applies both to the {@code key} parameter and returned value. Note that
     * this implies an empty array cannot be returned to indicate that no keys exist (instead, return null).
     *
     * <p>
     * This method must either return null or a value less than or equal to {@code key} (using the above interpretation
     * for empty arrays).
     *
     * @param key starting key, or an empty array to indicate a maximal upper bound
     * @return an upper bound (exclusive) for contained keys strictly less that {@code key},
     *  null if no key strictly less than {@code key} is contained by this instance, or an empty {@code byte[]} array
     *  to indicate an upper bound greater than all {@code byte[]} keys (which implies {@code key} was also empty)
     * @throws IllegalArgumentException if {@code key} is null
     */
    ByteData seekLower(ByteData key);
}
