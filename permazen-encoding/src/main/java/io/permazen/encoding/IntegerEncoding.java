
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import org.dellroad.stuff.java.Primitive;

/**
 * {@code int} primitive type.
 */
public class IntegerEncoding extends IntegralEncoding<Integer> {

    private static final long serialVersionUID = 1978611631822982974L;

    public IntegerEncoding(EncodingId encodingId) {
       super(encodingId, Primitive.INTEGER);
    }

    @Override
    public IntegerEncoding withEncodingId(EncodingId encodingId) {
        return new IntegerEncoding(encodingId);
    }

    @Override
    protected Integer convertNumber(Number value) {
        return value.intValue();
    }

    @Override
    protected Integer downCast(long value) {
        return (int)value;
    }

    @Override
    public Integer validate(Object obj) {
        if (obj instanceof Character)
            return (int)(Character)obj;
        if (obj instanceof Byte || obj instanceof Short)
            return ((Number)obj).intValue();
        return super.validate(obj);
    }
}
