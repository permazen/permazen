
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

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
     * @param value1Encoding component value encoding
     * @param value2Encoding component value encoding
     * @throws IllegalArgumentException if any component value encoding is null
     */
    @SuppressWarnings("serial")
    public Tuple2Encoding(Encoding<V1> value1Encoding, Encoding<V2> value2Encoding) {
        this(null, value1Encoding, value2Encoding);
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID, or null for an anonymous instance
     * @param value1Encoding component value encoding
     * @param value2Encoding component value encoding
     * @throws IllegalArgumentException if any component value encoding is null
     */
    @SuppressWarnings("serial")
    public Tuple2Encoding(EncodingId encodingId, Encoding<V1> value1Encoding, Encoding<V2> value2Encoding) {
        super(encodingId, new TypeToken<Tuple2<V1, V2>>() { }
           .where(new TypeParameter<V1>() { }, value1Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Encoding.getTypeToken().wrap()),
          value1Encoding, value2Encoding);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple2<V1, V2> createTuple(Object[] values) {
        assert values.length == 2;
        return new Tuple2<>((V1)values[0], (V2)values[1]);
    }
}
