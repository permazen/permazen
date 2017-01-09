
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;

/**
 * Short type.
 */
class ShortType extends IntegralType<Short> {

    private static final long serialVersionUID = 3817308228385115418L;

    ShortType() {
       super(Primitive.SHORT);
    }

    @Override
    protected Short convertNumber(Number value) {
        return value.shortValue();
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

