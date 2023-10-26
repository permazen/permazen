
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

/**
 * Implemented by tuples that have at least five values.
 */
public interface Has5<V1, V2, V3, V4, V5> extends Has4<V1, V2, V3, V4> {

    /**
     * Get the fifth value.
     *
     * @return fifth value in this tuple
     */
    V5 getValue5();
}
