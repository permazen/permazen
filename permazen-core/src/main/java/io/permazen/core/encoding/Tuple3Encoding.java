
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.Encoding;
import io.permazen.core.EncodingId;
import io.permazen.tuple.Tuple3;

/**
 * Composite encoding constructed from the concatenation of three component encodings.
 */
public class Tuple3Encoding<V1, V2, V3> extends TupleEncoding<Tuple3<V1, V2, V3>> {

    private static final long serialVersionUID = 4983105988201934382L;

    /**
     * Create an anonymous instance.
     *
     * @param value1Type component value encoding
     * @param value2Type component value encoding
     * @param value3Type component value encoding
     * @throws IllegalArgumentException if any component value type is null
     */
    @SuppressWarnings("serial")
    public Tuple3Encoding(Encoding<V1> value1Type, Encoding<V2> value2Type, Encoding<V3> value3Type) {
        this(null, value1Type, value2Type, value3Type);
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID, or null for an anonymous instance
     * @param value1Type component value encoding
     * @param value2Type component value encoding
     * @param value3Type component value encoding
     * @throws IllegalArgumentException if any component value type is null
     */
    @SuppressWarnings("serial")
    public Tuple3Encoding(EncodingId encodingId, Encoding<V1> value1Type, Encoding<V2> value2Type, Encoding<V3> value3Type) {
        super(encodingId, new TypeToken<Tuple3<V1, V2, V3>>() { }
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
