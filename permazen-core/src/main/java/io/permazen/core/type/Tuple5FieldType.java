
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.FieldType;
import io.permazen.tuple.Tuple5;

public class Tuple5FieldType<V1, V2, V3, V4, V5> extends TupleFieldType<Tuple5<V1, V2, V3, V4, V5>> {

    private static final long serialVersionUID = -3834483329232587435L;

    @SuppressWarnings("serial")
    public Tuple5FieldType(FieldType<V1> value1Type, FieldType<V2> value2Type, FieldType<V3> value3Type,
      FieldType<V4> value4Type, FieldType<V5> value5Type) {
        super(new TypeToken<Tuple5<V1, V2, V3, V4, V5>>() { }
           .where(new TypeParameter<V1>() { }, value1Type.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Type.getTypeToken().wrap())
           .where(new TypeParameter<V3>() { }, value3Type.getTypeToken().wrap())
           .where(new TypeParameter<V4>() { }, value4Type.getTypeToken().wrap())
           .where(new TypeParameter<V5>() { }, value5Type.getTypeToken().wrap()),
          value1Type, value2Type, value3Type, value4Type, value5Type);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple5<V1, V2, V3, V4, V5> createTuple(Object[] values) {
        assert values.length == 5;
        return new Tuple5<>((V1)values[0], (V2)values[1], (V3)values[2], (V4)values[3], (V5)values[4]);
    }
}

