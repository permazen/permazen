
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;

/**
 * Short type.
 */
class ShortType extends IntegralType<Short> {

    ShortType() {
       super(Primitive.SHORT);
    }

    @Override
    protected Short downCast(long value) {
        return (short)value;
    }

    @Override
    public Short validate(Object obj) {
        if (obj instanceof Byte)
            return ((Number)obj).shortValue();
        return super.validate(obj);
    }
}

