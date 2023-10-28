
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.Encoding;
import io.permazen.core.EncodingId;
import io.permazen.tuple.Tuple4;

/**
 * Composite encoding constructed from the concatenation of four component encodings.
 */
public class Tuple4Encoding<V1, V2, V3, V4> extends TupleEncoding<Tuple4<V1, V2, V3, V4>> {

    private static final long serialVersionUID = 7251327021306850353L;

    /**
     * Create an anonymous instance.
     *
     * @param value1Encoding component value encoding
     * @param value2Encoding component value encoding
     * @param value3Encoding component value encoding
     * @param value4Encoding component value encoding
     * @throws IllegalArgumentException if any component value encoding is null
     */
    @SuppressWarnings("serial")
    public Tuple4Encoding(Encoding<V1> value1Encoding,
      Encoding<V2> value2Encoding, Encoding<V3> value3Encoding, Encoding<V4> value4Encoding) {
        this(null, value1Encoding, value2Encoding, value3Encoding, value4Encoding);
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID, or null for an anonymous instance
     * @param value1Encoding component value encoding
     * @param value2Encoding component value encoding
     * @param value3Encoding component value encoding
     * @param value4Encoding component value encoding
     * @throws IllegalArgumentException if any component value encoding is null
     */
    @SuppressWarnings("serial")
    public Tuple4Encoding(EncodingId encodingId, Encoding<V1> value1Encoding,
      Encoding<V2> value2Encoding, Encoding<V3> value3Encoding, Encoding<V4> value4Encoding) {
        super(encodingId, new TypeToken<Tuple4<V1, V2, V3, V4>>() { }
           .where(new TypeParameter<V1>() { }, value1Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V3>() { }, value3Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V4>() { }, value4Encoding.getTypeToken().wrap()),
          value1Encoding, value2Encoding, value3Encoding, value4Encoding);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple4<V1, V2, V3, V4> createTuple(Object[] values) {
        assert values.length == 4;
        return new Tuple4<>((V1)values[0], (V2)values[1], (V3)values[2], (V4)values[3]);
    }
}
