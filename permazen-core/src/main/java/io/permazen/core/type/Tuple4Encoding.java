
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.Encoding;
import io.permazen.tuple.Tuple4;

public class Tuple4Encoding<V1, V2, V3, V4> extends TupleEncoding<Tuple4<V1, V2, V3, V4>> {

    private static final long serialVersionUID = 7251327021306850353L;

    @SuppressWarnings("serial")
    public Tuple4Encoding(Encoding<V1> value1Type, Encoding<V2> value2Type, Encoding<V3> value3Type, Encoding<V4> value4Type) {
        super(new TypeToken<Tuple4<V1, V2, V3, V4>>() { }
           .where(new TypeParameter<V1>() { }, value1Type.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Type.getTypeToken().wrap())
           .where(new TypeParameter<V3>() { }, value3Type.getTypeToken().wrap())
           .where(new TypeParameter<V4>() { }, value4Type.getTypeToken().wrap()),
          value1Type, value2Type, value3Type, value4Type);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple4<V1, V2, V3, V4> createTuple(Object[] values) {
        assert values.length == 4;
        return new Tuple4<>((V1)values[0], (V2)values[1], (V3)values[2], (V4)values[3]);
    }
}
