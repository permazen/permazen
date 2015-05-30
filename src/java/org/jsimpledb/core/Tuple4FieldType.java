
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import org.jsimpledb.tuple.Tuple4;

class Tuple4FieldType<V1, V2, V3, V4> extends TupleFieldType<Tuple4<V1, V2, V3, V4>> {

    @SuppressWarnings("serial")
    Tuple4FieldType(FieldType<V1> value1Type, FieldType<V2> value2Type, FieldType<V3> value3Type, FieldType<V4> value4Type) {
        super(new TypeToken<Tuple4<V1, V2, V3, V4>>() { }
           .where(new TypeParameter<V1>() { }, value1Type.typeToken.wrap())
           .where(new TypeParameter<V2>() { }, value2Type.typeToken.wrap())
           .where(new TypeParameter<V3>() { }, value3Type.typeToken.wrap())
           .where(new TypeParameter<V4>() { }, value4Type.typeToken.wrap()),
          value1Type, value2Type, value3Type, value4Type);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple4<V1, V2, V3, V4> createTuple(Object[] values) {
        assert values.length == 4;
        return new Tuple4<V1, V2, V3, V4>((V1)values[0], (V2)values[1], (V3)values[2], (V4)values[3]);
    }
}

