
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.KeyRange;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Represents a set of mutations that can be applied to a {@link io.permazen.kv.KVStore}.
 *
 * <p>
 * Each mutation is either a key/value put, the removal of a key range (possibly containing only a single key),
 * or a counter adjustment. Mutations are expected to be applied in the order: removes, puts, adjusts. Therefore,
 * if the same key is mutated in multiple ways, adjusts should occur after puts, and puts should occur after removes.
 */
public interface Mutations {

    /**
     * Get the key ranges removals contained by this instance.
     *
     * @return key ranges removed
     */
    Stream<KeyRange> getRemoveRanges();

    /**
     * Get the written key/value pairs contained by this instance.
     *
     * <p>
     * The caller must not modify any of the returned {@code byte[]} arrays.
     *
     * @return mapping from key to corresponding value
     */
    Stream<Map.Entry<byte[], byte[]>> getPutPairs();

    /**
     * Get the counter adjustments contained by this instance.
     *
     * <p>
     * The caller must not modify any of the returned {@code byte[]} arrays.
     *
     * @return mapping from key to corresponding counter adjustment
     */
    Stream<Map.Entry<byte[], Long>> getAdjustPairs();
}
