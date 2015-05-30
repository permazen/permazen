
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.tuple;

/**
 * A tuple of five values.
 *
 * <p>
 * Instances are immutable.
 * </p>
 *
 * @param <V1> first value type
 * @param <V2> second value type
 * @param <V3> third value type
 * @param <V4> fourth value type
 * @param <V5> fifth value type
 */
public class Tuple5<V1, V2, V3, V4, V5> extends AbstractHas5<V1, V2, V3, V4, V5> {

    /**
     * Constructor.
     *
     * @param v1 the first value
     * @param v2 the second value
     * @param v3 the third value
     * @param v4 the fouth value
     * @param v5 the fifth value
     */
    public Tuple5(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5) {
        super(v1, v2, v3, v4, v5);
    }
}

