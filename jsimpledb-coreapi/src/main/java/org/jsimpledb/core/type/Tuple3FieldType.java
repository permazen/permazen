
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.tuple.Tuple3;

public class Tuple3FieldType<V1, V2, V3> extends TupleFieldType<Tuple3<V1, V2, V3>> {

    private static final long serialVersionUID = 4983105988201934382L;

    @SuppressWarnings("serial")
    public Tuple3FieldType(FieldType<V1> value1Type, FieldType<V2> value2Type, FieldType<V3> value3Type) {
        super(new TypeToken<Tuple3<V1, V2, V3>>() { }
           .where(new TypeParameter<V1>() { }, value1Type.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Type.getTypeToken().wrap())
           .where(new TypeParameter<V3>() { }, value3Type.getTypeToken().wrap()),
          value1Type, value2Type, value3Type);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple3<V1, V2, V3> createTuple(Object[] values) {
        assert values.length == 3;
        return new Tuple3<>((V1)values[0], (V2)values[1], (V3)values[2]);
    }
}

