
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.index;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.tuple.Tuple2;

/**
 * An index on a single field.
 *
 * <p>
 * Indexes are read-only and "live", always reflecting the current transaction state.
 * </p>
 *
 * @param <V> indexed value type
 * @param <T> index target type
 * @see org.jsimpledb.index
 */
public interface Index<V, T> {

    /**
     * View this index as a {@link NavigableSet} of tuples.
     *
     * @return {@link NavigableSet} of tuples containing indexed value and target value
     */
    NavigableSet<Tuple2<V, T>> asSet();

    /**
     * View this index as a {@link NavigableMap} of target values keyed by indexed value.
     *
     * @return {@link NavigableMap} from indexed value to the corresponding set of target objects
     */
    NavigableMap<V, NavigableSet<T>> asMap();
}

