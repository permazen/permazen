
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.tuple.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Support superclass for non-null {@link Encoding}s of values that can be decomposed into two component values.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * @param <T> this encoding's value type
 * @param <V1> first tuple value type
 * @param <V2> second tuple value type
 */
public abstract class Concat2Encoding<T, V1, V2> extends ConvertedEncoding<T, Tuple2<V1, V2>> {

    private static final long serialVersionUID = -7395218884659436173L;

    /**
     * Constructor.
     *
     * @param type Java type for this encoding's values
     * @param encoding1 first value encoding
     * @param encoding2 second value encoding
     * @param splitter1 first value splitter
     * @param splitter2 second value splitter
     * @param joiner value joiner from tuple
     * @throws IllegalArgumentException if any parameter is null
     */
    protected Concat2Encoding(Class<T> type,
      Encoding<V1> encoding1, Encoding<V2> encoding2,
      Function<? super T, ? extends V1> splitter1, Function<? super T, ? extends V2> splitter2,
      BiFunction<? super V1, ? super V2, ? extends T> joiner) {
        super(null, type, null, new Tuple2Encoding<>(encoding1, encoding2),
          Converter.from(
            value -> new Tuple2<>(splitter1.apply(value), splitter2.apply(value)),
            tuple -> joiner.apply(tuple.getValue1(), tuple.getValue2())),
          false);
        Preconditions.checkArgument(splitter1 != null, "null splitter1");
        Preconditions.checkArgument(splitter2 != null, "null splitter2");
        Preconditions.checkArgument(joiner != null, "null joiner");
    }

    @SuppressWarnings("unchecked")
    public Tuple2Encoding<V1, V2> getTuple2Encoding() {
        return (Tuple2Encoding<V1, V2>)this.delegate;
    }
}
