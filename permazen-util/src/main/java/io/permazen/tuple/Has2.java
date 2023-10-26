
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

/**
 * Implemented by tuples that have at least two values.
 */
public interface Has2<V1, V2> extends Has1<V1> {

    /**
     * Get the second value.
     *
     * @return second value in this tuple
     */
    V2 getValue2();
}
