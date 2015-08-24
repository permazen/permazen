
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.tuple;

/**
 * A tuple of three values.
 *
 * <p>
 * Instances are immutable.
 * </p>
 *
 * @param <V1> first value type
 * @param <V2> second value type
 * @param <V3> third value type
 */
public class Tuple3<V1, V2, V3> extends AbstractHas3<V1, V2, V3> {

    /**
     * Constructor.
     *
     * @param v1 the first value
     * @param v2 the second value
     * @param v3 the third value
     */
    public Tuple3(V1 v1, V2 v2, V3 v3) {
        super(v1, v2, v3);
    }
}

