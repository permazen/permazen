
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.tuple.Tuple5;

/**
 * Composite encoding constructed from the concatenation of five component encodings.
 */
public class Tuple5Encoding<V1, V2, V3, V4, V5> extends TupleEncoding<Tuple5<V1, V2, V3, V4, V5>> {

    private static final long serialVersionUID = -3834483329232587435L;

    /**
     * Create an anonymous instance.
     *
     * @param value1Encoding component value encoding
     * @param value2Encoding component value encoding
     * @param value3Encoding component value encoding
     * @param value4Encoding component value encoding
     * @param value5Encoding component value encoding
     * @throws IllegalArgumentException if any component value encoding is null
     */
    @SuppressWarnings("serial")
    public Tuple5Encoding(Encoding<V1> value1Encoding, Encoding<V2> value2Encoding,
      Encoding<V3> value3Encoding, Encoding<V4> value4Encoding, Encoding<V5> value5Encoding) {
        this(null, value1Encoding, value2Encoding, value3Encoding, value4Encoding, value5Encoding);
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID, or null for an anonymous instance
     * @param value1Encoding component value encoding
     * @param value2Encoding component value encoding
     * @param value3Encoding component value encoding
     * @param value4Encoding component value encoding
     * @param value5Encoding component value encoding
     * @throws IllegalArgumentException if any component value encoding is null
     */
    @SuppressWarnings("serial")
    public Tuple5Encoding(EncodingId encodingId, Encoding<V1> value1Encoding,
      Encoding<V2> value2Encoding, Encoding<V3> value3Encoding, Encoding<V4> value4Encoding, Encoding<V5> value5Encoding) {
        super(encodingId, new TypeToken<Tuple5<V1, V2, V3, V4, V5>>() { }
           .where(new TypeParameter<V1>() { }, value1Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V3>() { }, value3Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V4>() { }, value4Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V5>() { }, value5Encoding.getTypeToken().wrap()),
          value1Encoding, value2Encoding, value3Encoding, value4Encoding, value5Encoding);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple5<V1, V2, V3, V4, V5> createTuple(Object[] values) {
        assert values.length == 5;
        return new Tuple5<>((V1)values[0], (V2)values[1], (V3)values[2], (V4)values[3], (V5)values[4]);
    }
}
