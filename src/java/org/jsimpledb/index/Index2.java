
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.index;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;

/**
 * An index on two fields.
 *
 * <p>
 * Indexes are read-only and "live", always reflecting the current transaction state.
 * </p>
 *
 * @param <V1> first indexed value type
 * @param <V2> second indexed value type
 * @param <T> index target type
 * @see org.jsimpledb.index
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
}

