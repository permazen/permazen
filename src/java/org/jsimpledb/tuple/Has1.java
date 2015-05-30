
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.tuple;

/**
 * Implemented by tuples that have a first value.
 */
public interface Has1<V1> {

    /**
     * Get the first value.
     *
     * @return first value in this tuple
     */
    V1 getValue1();
}

