
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

/**
 * Implemented by tuples that have at least four values.
 */
public interface Has4<V1, V2, V3, V4> extends Has3<V1, V2, V3> {

    /**
     * Get the fourth value.
     *
     * @return fourth value in this tuple
     */
    V4 getValue4();
}
