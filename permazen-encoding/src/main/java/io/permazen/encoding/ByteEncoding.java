
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import org.dellroad.stuff.java.Primitive;

/**
 * {@code byte} primitive type.
 */
public class ByteEncoding extends IntegralEncoding<Byte> {

    private static final long serialVersionUID = 7891495286075980831L;

    public ByteEncoding(EncodingId encodingId) {
       super(encodingId, Primitive.BYTE);
    }

    @Override
    public ByteEncoding withEncodingId(EncodingId encodingId) {
        return new ByteEncoding(encodingId);
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
