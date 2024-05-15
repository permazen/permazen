
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.tuple.Tuple2;

/**
 * Non-null {@link Encoding} for a {@link Tuple2} created by concatenating the component {@link Encoding}s.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 */
public class Tuple2Encoding<V1, V2> extends TupleEncoding<Tuple2<V1, V2>> {

    private static final long serialVersionUID = 8336238765491523439L;

    /**
     * Constructor.
     *
     * @param value1Encoding component value encoding
     * @param value2Encoding component value encoding
     * @throws IllegalArgumentException if any component value encoding is null
     */
    @SuppressWarnings("serial")
    public Tuple2Encoding(Encoding<V1> value1Encoding, Encoding<V2> value2Encoding) {
        super(new TypeToken<Tuple2<V1, V2>>() { }
           .where(new TypeParameter<V1>() { }, value1Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Encoding.getTypeToken().wrap()),
          value1Encoding, value2Encoding);
    }

    @SuppressWarnings("unchecked")
    public Encoding<V1> getEncoding1() {
        return (Encoding<V1>)this.encodings.get(0);
    }

    @SuppressWarnings("unchecked")
    public Encoding<V2> getEncoding2() {
        return (Encoding<V2>)this.encodings.get(1);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple2<V1, V2> createTuple(Object[] values) {
        assert values.length == 2;
        return new Tuple2<>((V1)values[0], (V2)values[1]);
    }
}
