
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

/**
 * A "tuple" of one value.
 *
 * <p>
 * Instances are immutable.
 *
 * @param <V1> value type
 */
public class Tuple1<V1> extends AbstractHas1<V1> {

    /**
     * Constructor.
     *
     * @param v1 the value
     */
    public Tuple1(V1 v1) {
        super(v1);
    }
}
