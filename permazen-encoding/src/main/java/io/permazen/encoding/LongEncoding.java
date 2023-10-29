
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import org.dellroad.stuff.java.Primitive;

/**
 * Long type.
 */
public class LongEncoding extends IntegralEncoding<Long> {

    private static final long serialVersionUID = -1090469104525478415L;

    public LongEncoding() {
       super(Primitive.LONG);
    }

    @Override
    protected Long convertNumber(Number value) {
        return value.longValue();
    }

    @Override
    protected Long downCast(long value) {
        return value;
    }

    @Override
    public Long validate(Object obj) {
        if (obj instanceof Character)
            return (long)(Character)obj;
        if (obj instanceof Byte || obj instanceof Short || obj instanceof Integer)
            return ((Number)obj).longValue();
        return super.validate(obj);
    }
}
