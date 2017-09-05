
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.index;

import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.tuple.Tuple4;
import io.permazen.tuple.Tuple5;
import io.permazen.util.Bounds;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * An index on a four fields.
 *
 * <p>
 * Indexes are read-only and "live", always reflecting the current transaction state.
 *
 * @param <V1> first indexed value type
 * @param <V2> second indexed value type
 * @param <V3> third indexed value type
 * @param <V4> fourth indexed value type
 * @param <T> index target type
 * @see io.permazen.index
 */
public interface Index4<V1, V2, V3, V4, T> {

    /**
     * View this index as a {@link NavigableSet} of tuples.
     *
     * @return {@link NavigableSet} of tuples containing indexed values and target value
     */
    NavigableSet<Tuple5<V1, V2, V3, V4, T>> asSet();

    /**
     * View this index as a {@link NavigableMap} of target values keyed by indexed value tuples.
     *
     * @return {@link NavigableMap} from indexed value tuple to the corresponding set of target objects
     */
    NavigableMap<Tuple4<V1, V2, V3, V4>, NavigableSet<T>> asMap();

    /**
     * View this index as a {@link NavigableMap} of {@link Index}s keyed by the first three values.
     *
     * @return {@link NavigableMap} from first three values to {@link Index}
     */
    NavigableMap<Tuple3<V1, V2, V3>, Index<V4, T>> asMapOfIndex();

    /**
     * View this index as a {@link NavigableMap} of {@link Index2}s keyed by the first two values.
     *
     * @return {@link NavigableMap} from first two values to {@link Index2}
     */
    NavigableMap<Tuple2<V1, V2>, Index2<V3, V4, T>> asMapOfIndex2();

    /**
     * View this index as a {@link NavigableMap} of {@link Index3}s keyed by the first value.
     *
     * @return {@link NavigableMap} from first value to {@link Index3}
     */
    NavigableMap<V1, Index3<V2, V3, V4, T>> asMapOfIndex3();

    /**
     * Get the prefix of this instance that only includes the first four indexed fields.
     *
     * @return prefix of this index
     */
    Index3<V1, V2, V3, V4> asIndex3();

    /**
     * Get the prefix of this instance that only includes the first three indexed fields.
     *
     * @return prefix of this index
     */
    Index2<V1, V2, V3> asIndex2();

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
    Index4<V1, V2, V3, V4, T> withValue1Bounds(Bounds<V1> bounds);

    /**
     * Impose {@link Bounds} that restrict the range of the second indexed value.
     *
     * @param bounds bounds to impose on the second indexed value
     * @return a view of this index in which only second values within {@code bounds} are visible
     */
    Index4<V1, V2, V3, V4, T> withValue2Bounds(Bounds<V2> bounds);

    /**
     * Impose {@link Bounds} that restrict the range of the third indexed value.
     *
     * @param bounds bounds to impose on the third indexed value
     * @return a view of this index in which only third values within {@code bounds} are visible
     */
    Index4<V1, V2, V3, V4, T> withValue3Bounds(Bounds<V3> bounds);

    /**
     * Impose {@link Bounds} that restrict the range of the fourth indexed value.
     *
     * @param bounds bounds to impose on the fourth indexed value
     * @return a view of this index in which only fourth values within {@code bounds} are visible
     */
    Index4<V1, V2, V3, V4, T> withValue4Bounds(Bounds<V4> bounds);

    /**
     * Impose {@link Bounds} that restrict the range of the target value.
     *
     * @param bounds bounds to impose on the target value
     * @return a view of this index in which only target values within {@code bounds} are visible
     */
    Index4<V1, V2, V3, V4, T> withTargetBounds(Bounds<T> bounds);
}

