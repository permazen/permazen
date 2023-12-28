
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.index;

import io.permazen.tuple.Tuple;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * An index on a one or more fields in some target type.
 *
 * <p>
 * Indexes are read-only and "live", always reflecting the current transaction state.
 *
 * @param <T> index target type
 * @see io.permazen.index
 */
public interface Index<T> {

    /**
     * Get the number of fields in this index.
     *
     * @return the number of indexed fields
     */
    int numberOfFields();

    /**
     * View this index as a {@link NavigableSet} of tuples.
     *
     * @return {@link NavigableSet} of tuples containing indexed value(s) and target value
     */
    NavigableSet<? extends Tuple> asSet();

    /**
     * View this index as a {@link NavigableMap} of target values keyed by indexed value(s).
     *
     * @return {@link NavigableMap} from indexed value(s) to the corresponding set of target objects
     */
    NavigableMap<?, NavigableSet<T>> asMap();
}
