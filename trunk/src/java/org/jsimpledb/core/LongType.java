
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

    @Override
    public Long validate(Object obj) {
        if (obj instanceof Character)
            return (long)((Character)obj).charValue();
        if (obj instanceof Byte || obj instanceof Short || obj instanceof Integer)
            return ((Number)obj).longValue();
        return super.validate(obj);
    }
}

