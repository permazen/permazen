
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.tuple.Tuple3;

import java.util.function.Function;

/**
 * Support superclass for non-null {@link Encoding}s of values that can be decomposed into three component values.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * @param <T> this encoding's value type
 * @param <V1> first tuple value type
 * @param <V2> second tuple value type
 * @param <V3> third tuple value type
 */
public abstract class Concat3Encoding<T, V1, V2, V3> extends ConvertedEncoding<T, Tuple3<V1, V2, V3>> {

    private static final long serialVersionUID = -7395218884659436174L;

    /**
     * Constructor.
     *
     * @param type Java type for this encoding's values
     * @param encoding1 first value encoding
     * @param encoding2 second value encoding
     * @param encoding3 third value encoding
     * @param splitter1 first value splitter
     * @param splitter2 second value splitter
     * @param splitter3 third value splitter
     * @param joiner value joiner from tuple
     * @throws IllegalArgumentException if any parameter is null
     */
    protected Concat3Encoding(Class<T> type,
      Encoding<V1> encoding1,
      Encoding<V2> encoding2,
      Encoding<V3> encoding3,
      Function<? super T, ? extends V1> splitter1,
      Function<? super T, ? extends V2> splitter2,
      Function<? super T, ? extends V3> splitter3,
      Function<? super Tuple3<V1, V2, V3>, ? extends T> joiner) {
        super(null, type, null, new Tuple3Encoding<>(encoding1, encoding2, encoding3),
          Converter.from(
            value -> new Tuple3<>(splitter1.apply(value), splitter2.apply(value), splitter3.apply(value)),
            joiner::apply),
          false);
        Preconditions.checkArgument(splitter1 != null, "null splitter1");
        Preconditions.checkArgument(splitter2 != null, "null splitter2");
        Preconditions.checkArgument(splitter3 != null, "null splitter3");
        Preconditions.checkArgument(joiner != null, "null joiner");
    }

    @SuppressWarnings("unchecked")
    public Tuple3Encoding<V1, V2, V3> getTuple3Encoding() {
        return (Tuple3Encoding<V1, V2, V3>)this.delegate;
    }
}
