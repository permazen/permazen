
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.tuple;

/**
 * A tuple of four values.
 *
 * <p>
 * Instances are immutable.
 * </p>
 *
 * @param <V1> first value type
 * @param <V2> second value type
 * @param <V3> third value type
 * @param <V4> fourth value type
 */
public class Tuple4<V1, V2, V3, V4> extends AbstractHas4<V1, V2, V3, V4> {

    /**
     * Constructor.
     *
     * @param v1 the first value
     * @param v2 the second value
     * @param v3 the third value
     * @param v4 the fouth value
     */
    public Tuple4(V1 v1, V2 v2, V3 v3, V4 v4) {
        super(v1, v2, v3, v4);
    }
}

