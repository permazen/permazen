
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import org.dellroad.stuff.java.Primitive;

/**
 * {@code byte} primitive type.
 */
public class ByteType extends IntegralType<Byte> {

    private static final long serialVersionUID = 7891495286075980831L;

    public ByteType() {
       super(Primitive.BYTE);
    }

    @Override
    protected Byte convertNumber(Number value) {
        return value.byteValue();
    }

    @Override
    protected Byte downCast(long value) {
        return (byte)value;
    }
}

