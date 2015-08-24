
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.tuple;

import java.util.List;

/**
 * Implemented by all "tuple" classes.
 */
public interface Tuple {

    /**
     * Get the number of values in this tuple.
     *
     * @return the size of this tuple
     */
    int getSize();

    /**
     * View this instance as a list.
     *
     * @return unmodifiable {@link List} view of this tuple
     */
    List<Object> asList();

    /**
     * Compare for equality.
     *
     * <p>
     * Tuples are equal if they have the same size and the corresponding individual values are equal (or null).
     * </p>
     */
    @Override
    boolean equals(Object obj);

    /**
     * Compute hash code.
     *
     * <p>
     * The hash code of a tuple is the exclusive-OR of the hash codes of the individual values (using zero for null values).
     * </p>
     */
    @Override
    int hashCode();
}

