
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.tuple.Tuple5;

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
 * @param <V5> fifth tuple value type
 */
public abstract class Concat5Encoding<T, V1, V2, V3, V4, V5> extends ConvertedEncoding<T, Tuple5<V1, V2, V3, V4, V5>> {

    private static final long serialVersionUID = -7395218884659436176L;

    /**
     * Constructor.
     *
     * @param type Java type for this encoding's values
     * @param encoding1 first value encoding
     * @param encoding2 second value encoding
     * @param encoding3 third value encoding
     * @param encoding4 fourth value encoding
     * @param encoding5 fifth value encoding
     * @param splitter1 first value splitter
     * @param splitter2 second value splitter
     * @param splitter3 third value splitter
     * @param splitter4 fourth value splitter
     * @param splitter5 fifth value splitter
     * @param joiner value joiner from tuple
     * @throws IllegalArgumentException if any parameter is null
     */
    protected Concat5Encoding(Class<T> type,
      Encoding<V1> encoding1,
      Encoding<V2> encoding2,
      Encoding<V3> encoding3,
      Encoding<V4> encoding4,
      Encoding<V5> encoding5,
      Function<? super T, ? extends V1> splitter1,
      Function<? super T, ? extends V2> splitter2,
      Function<? super T, ? extends V3> splitter3,
      Function<? super T, ? extends V4> splitter4,
      Function<? super T, ? extends V5> splitter5,
      Function<? super Tuple5<V1, V2, V3, V4, V5>, ? extends T> joiner) {
        super(null, type, null, new Tuple5Encoding<>(encoding1, encoding2, encoding3, encoding4, encoding5),
          Converter.from(
            value -> new Tuple5<>(splitter1.apply(value),
              splitter2.apply(value), splitter3.apply(value), splitter4.apply(value), splitter5.apply(value)),
            joiner::apply),
          false);
        Preconditions.checkArgument(splitter1 != null, "null splitter1");
        Preconditions.checkArgument(splitter2 != null, "null splitter2");
        Preconditions.checkArgument(splitter3 != null, "null splitter3");
        Preconditions.checkArgument(splitter4 != null, "null splitter4");
        Preconditions.checkArgument(splitter5 != null, "null splitter5");
        Preconditions.checkArgument(joiner != null, "null joiner");
    }

    @SuppressWarnings("unchecked")
    public Tuple5Encoding<V1, V2, V3, V4, V5> getTuple5Encoding() {
        return (Tuple5Encoding<V1, V2, V3, V4, V5>)this.delegate;
    }
}
