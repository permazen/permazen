
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

/**
 * Implemented by tuples that have at least three values.
 */
public interface Has3<V1, V2, V3> extends Has2<V1, V2> {

    /**
     * Get the third value.
     *
     * @return third value in this tuple
     */
    V3 getValue3();
}
