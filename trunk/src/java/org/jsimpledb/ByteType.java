
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.dellroad.stuff.java.Primitive;

/**
 * {@code byte} primitive type.
 */
class ByteType extends IntegralType<Byte> {

    ByteType() {
       super(Primitive.BYTE);
    }

    @Override
    protected Byte downCast(long value) {
        return (byte)value;
    }
}

