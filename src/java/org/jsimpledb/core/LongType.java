
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;

/**
 * Long type.
 */
class LongType extends IntegralType<Long> {

    LongType() {
       super(Primitive.LONG);
    }

    @Override
    protected Long downCast(long value) {
        return value;
    }
}

