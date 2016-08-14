
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;

/**
 * {@code byte} primitive type.
 */
class ByteType extends IntegralType<Byte> {

    private static final long serialVersionUID = 7891495286075980831L;

    ByteType() {
       super(Primitive.BYTE);
    }

    @Override
    protected Byte downCast(long value) {
        return (byte)value;
    }
}

