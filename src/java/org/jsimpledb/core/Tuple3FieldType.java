
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import org.jsimpledb.tuple.Tuple3;

class Tuple3FieldType<V1, V2, V3> extends TupleFieldType<Tuple3<V1, V2, V3>> {

    @SuppressWarnings("serial")
    Tuple3FieldType(FieldType<V1> value1Type, FieldType<V2> value2Type, FieldType<V3> value3Type) {
        super(new TypeToken<Tuple3<V1, V2, V3>>() { }
           .where(new TypeParameter<V1>() { }, value1Type.typeToken.wrap())
           .where(new TypeParameter<V2>() { }, value2Type.typeToken.wrap())
           .where(new TypeParameter<V3>() { }, value3Type.typeToken.wrap()),
          value1Type, value2Type, value3Type);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple3<V1, V2, V3> createTuple(Object[] values) {
        assert values.length == 3;
        return new Tuple3<V1, V2, V3>((V1)values[0], (V2)values[1], (V3)values[2]);
    }
}

