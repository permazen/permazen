
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import org.dellroad.stuff.java.Primitive;

/**
 * Short type.
 */
public class ShortEncoding extends IntegralEncoding<Short> {

    private static final long serialVersionUID = 3817308228385115418L;

    public ShortEncoding(EncodingId encodingId) {
       super(encodingId, Primitive.SHORT);
    }

    @Override
    public ShortEncoding withEncodingId(EncodingId encodingId) {
        return new ShortEncoding(encodingId);
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
