
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.tuple.Tuple4;

import java.util.function.Function;

/**
 * Support superclass for non-null {@link Encoding}s of values that can be decomposed into four component values.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * @param <T> this encoding's value type
 * @param <V1> first tuple value type
 * @param <V2> second tuple value type
 * @param <V3> third tuple value type
 * @param <V4> fourth tuple value type
 */
public abstract class Concat4Encoding<T, V1, V2, V3, V4> extends ConvertedEncoding<T, Tuple4<V1, V2, V3, V4>> {

    private static final long serialVersionUID = -7395218884659436175L;

    /**
     * Constructor.
     *
     * @param type Java type for this encoding's values
     * @param encoding1 first value encoding
     * @param encoding2 second value encoding
     * @param encoding3 third value encoding
     * @param encoding4 fourth value encoding
     * @param splitter1 first value splitter
     * @param splitter2 second value splitter
     * @param splitter3 third value splitter
     * @param splitter4 fourth value splitter
     * @param joiner value joiner from tuple
     * @throws IllegalArgumentException if any parameter is null
     */
    protected Concat4Encoding(Class<T> type,
      Encoding<V1> encoding1,
      Encoding<V2> encoding2,
      Encoding<V3> encoding3,
      Encoding<V4> encoding4,
      Function<? super T, ? extends V1> splitter1,
      Function<? super T, ? extends V2> splitter2,
      Function<? super T, ? extends V3> splitter3,
      Function<? super T, ? extends V4> splitter4,
      Function<? super Tuple4<V1, V2, V3, V4>, ? extends T> joiner) {
        super(null, type, new Tuple4Encoding<>(encoding1, encoding2, encoding3, encoding4),
          Converter.from(
            value -> new Tuple4<>(splitter1.apply(value), splitter2.apply(value), splitter3.apply(value), splitter4.apply(value)),
            joiner::apply));
        Preconditions.checkArgument(splitter1 != null, "null splitter1");
        Preconditions.checkArgument(splitter2 != null, "null splitter2");
        Preconditions.checkArgument(splitter3 != null, "null splitter3");
        Preconditions.checkArgument(splitter4 != null, "null splitter4");
        Preconditions.checkArgument(joiner != null, "null joiner");
    }

    @SuppressWarnings("unchecked")
    public Tuple4Encoding<V1, V2, V3, V4> getTuple4Encoding() {
        return (Tuple4Encoding<V1, V2, V3, V4>)this.delegate;
    }
}
