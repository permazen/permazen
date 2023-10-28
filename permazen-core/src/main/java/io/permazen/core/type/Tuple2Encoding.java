
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.Encoding;
import io.permazen.core.EncodingId;
import io.permazen.tuple.Tuple2;

/**
 * Composite encoding constructed from the concatenation of two component encodings.
 */
public class Tuple2Encoding<V1, V2> extends TupleEncoding<Tuple2<V1, V2>> {

    private static final long serialVersionUID = 8336238765491523439L;

    /**
     * Create an anonymous instance.
     *
     * @param typeToken this encoding's composite value type
     * @param encodings encodings to concatenate
     * @throws IllegalArgumentException if {@code typeToken} or {@link encodings} is null
     */
    @SuppressWarnings("serial")
    public Tuple2Encoding(Encoding<V1> value1Type, Encoding<V2> value2Type) {
        this(null, value1Type, value2Type);
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID, or null for an anonymous instance
     * @param encodings encodings to concatenate
     * @throws IllegalArgumentException if {@link encodings} is null
     */
    @SuppressWarnings("serial")
    public Tuple2Encoding(EncodingId encodingId, Encoding<V1> value1Type, Encoding<V2> value2Type) {
        super(encodingId, new TypeToken<Tuple2<V1, V2>>() { }
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
