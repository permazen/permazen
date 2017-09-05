
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.index;

import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.util.Bounds;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * An index on two fields.
 *
 * <p>
 * Indexes are read-only and "live", always reflecting the current transaction state.
 *
 * @param <V1> first indexed value type
 * @param <V2> second indexed value type
 * @param <T> index target type
 * @see io.permazen.index
 */
public interface Index2<V1, V2, T> {

    /**
     * View this index as a {@link NavigableSet} of tuples.
     *
     * @return {@link NavigableSet} of tuples containing indexed values and target value
     */
    NavigableSet<Tuple3<V1, V2, T>> asSet();

    /**
     * View this index as a {@link NavigableMap} of target values keyed by indexed value tuples.
     *
     * @return {@link NavigableMap} from indexed value tuple to the corresponding set of target objects
     */
    NavigableMap<Tuple2<V1, V2>, NavigableSet<T>> asMap();

    /**
     * View this index as a {@link NavigableMap} of {@link Index}s keyed by the first value.
     *
     * @return {@link NavigableMap} from first value to {@link Index}
     */
    NavigableMap<V1, Index<V2, T>> asMapOfIndex();

    /**
     * Get the prefix of this instance that only includes the first two indexed fields.
     *
     * @return prefix of this index
     */
    Index<V1, V2> asIndex();

    /**
     * Impose {@link Bounds} that restrict the range of the first indexed value.
     *
     * @param bounds bounds to impose on the first indexed value
     * @return a view of this index in which only first values within {@code bounds} are visible
     */
    Index2<V1, V2, T> withValue1Bounds(Bounds<V1> bounds);

    /**
     * Impose {@link Bounds} that restrict the range of the second indexed value.
     *
     * @param bounds bounds to impose on the second indexed value
     * @return a view of this index in which only second values within {@code bounds} are visible
     */
    Index2<V1, V2, T> withValue2Bounds(Bounds<V2> bounds);

    /**
     * Impose {@link Bounds} that restrict the range of the target value.
     *
     * @param bounds bounds to impose on the target value
     * @return a view of this index in which only target values within {@code bounds} are visible
     */
    Index2<V1, V2, T> withTargetBounds(Bounds<T> bounds);
}

