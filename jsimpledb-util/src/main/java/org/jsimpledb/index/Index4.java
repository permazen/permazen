
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.index;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;
import org.jsimpledb.tuple.Tuple4;
import org.jsimpledb.tuple.Tuple5;

/**
 * An index on a four fields.
 *
 * <p>
 * Indexes are read-only and "live", always reflecting the current transaction state.
 * </p>
 *
 * @param <V1> first indexed value type
 * @param <V2> second indexed value type
 * @param <V3> third indexed value type
 * @param <V4> fourth indexed value type
 * @param <T> index target type
 * @see org.jsimpledb.index
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
}

