
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.tuple.Tuple3;

/**
 * Non-null {@link Encoding} for a {@link Tuple3} created by concatenating the component {@link Encoding}s.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 */
public class Tuple3Encoding<V1, V2, V3> extends TupleEncoding<Tuple3<V1, V2, V3>> {

    private static final long serialVersionUID = 4983105988201934382L;

    /**
     * Constructor.
     *
     * @param value1Encoding component value encoding
     * @param value2Encoding component value encoding
     * @param value3Encoding component value encoding
     * @throws IllegalArgumentException if any component value encoding is null
     */
    @SuppressWarnings("serial")
    public Tuple3Encoding(Encoding<V1> value1Encoding, Encoding<V2> value2Encoding, Encoding<V3> value3Encoding) {
        super(new TypeToken<Tuple3<V1, V2, V3>>() { }
           .where(new TypeParameter<V1>() { }, value1Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V2>() { }, value2Encoding.getTypeToken().wrap())
           .where(new TypeParameter<V3>() { }, value3Encoding.getTypeToken().wrap()),
          value1Encoding, value2Encoding, value3Encoding);
    }

    @SuppressWarnings("unchecked")
    public Encoding<V1> getEncoding1() {
        return (Encoding<V1>)this.encodings.get(0);
    }

    @SuppressWarnings("unchecked")
    public Encoding<V2> getEncoding2() {
        return (Encoding<V2>)this.encodings.get(1);
    }

    @SuppressWarnings("unchecked")
    public Encoding<V3> getEncoding3() {
        return (Encoding<V3>)this.encodings.get(2);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple3<V1, V2, V3> createTuple(Object[] values) {
        assert values.length == 3;
        return new Tuple3<>((V1)values[0], (V2)values[1], (V3)values[2]);
    }
}
