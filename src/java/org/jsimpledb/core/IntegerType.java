
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;

/**
 * {@code int} primitive type.
 */
class IntegerType extends IntegralType<Integer> {

    IntegerType() {
       super(Primitive.INTEGER);
    }

    @Override
    protected Integer downCast(long value) {
        return (int)value;
    }
}

