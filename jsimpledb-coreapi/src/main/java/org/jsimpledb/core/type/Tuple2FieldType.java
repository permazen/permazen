
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.tuple.Tuple2;

public class Tuple2FieldType<V1, V2> extends TupleFieldType<Tuple2<V1, V2>> {

    private static final long serialVersionUID = 8336238765491523439L;

    @SuppressWarnings("serial")
    public Tuple2FieldType(FieldType<V1> value1Type, FieldType<V2> value2Type) {
        super(new TypeToken<Tuple2<V1, V2>>() { }
           .where(new TypeParameter<V1>() { }, value1Type.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Type.getTypeToken().wrap()),
          value1Type, value2Type);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple2<V1, V2> createTuple(Object[] values) {
        assert values.length == 2;
        return new Tuple2<>((V1)values[0], (V2)values[1]);
    }
}

