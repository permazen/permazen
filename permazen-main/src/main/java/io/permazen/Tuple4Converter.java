
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.tuple.Tuple4;

/**
 * Converts {@link Tuple4}s.
 *
 * @param <V1> first converted value
 * @param <V2> second converted value
 * @param <V3> third converted value
 * @param <V4> fouth converted value
 * @param <W1> first wrapped value
 * @param <W2> second wrapped value
 * @param <W3> third wrapped value
 * @param <W4> fourth wrapped value
 */
class Tuple4Converter<V1, V2, V3, V4, W1, W2, W3, W4> extends Converter<Tuple4<V1, V2, V3, V4>, Tuple4<W1, W2, W3, W4>> {

    private final Converter<V1, W1> value1Converter;
    private final Converter<V2, W2> value2Converter;
    private final Converter<V3, W3> value3Converter;
    private final Converter<V4, W4> value4Converter;

    Tuple4Converter(Converter<V1, W1> value1Converter, Converter<V2, W2> value2Converter,
      Converter<V3, W3> value3Converter, Converter<V4, W4> value4Converter) {
        Preconditions.checkArgument(value1Converter != null, "null value1Converter");
        Preconditions.checkArgument(value2Converter != null, "null value2Converter");
        Preconditions.checkArgument(value3Converter != null, "null value3Converter");
        Preconditions.checkArgument(value4Converter != null, "null value4Converter");
        this.value1Converter = value1Converter;
        this.value2Converter = value2Converter;
        this.value3Converter = value3Converter;
        this.value4Converter = value4Converter;
    }

    @Override
    protected Tuple4<W1, W2, W3, W4> doForward(Tuple4<V1, V2, V3, V4> tuple) {
        if (tuple == null)
            return null;
        return new Tuple4<W1, W2, W3, W4>(
          this.value1Converter.convert(tuple.getValue1()),
          this.value2Converter.convert(tuple.getValue2()),
          this.value3Converter.convert(tuple.getValue3()),
          this.value4Converter.convert(tuple.getValue4()));
    }

    @Override
    protected Tuple4<V1, V2, V3, V4> doBackward(Tuple4<W1, W2, W3, W4> tuple) {
        if (tuple == null)
            return null;
        return new Tuple4<V1, V2, V3, V4>(
          this.value1Converter.reverse().convert(tuple.getValue1()),
          this.value2Converter.reverse().convert(tuple.getValue2()),
          this.value3Converter.reverse().convert(tuple.getValue3()),
          this.value4Converter.reverse().convert(tuple.getValue4()));
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Tuple4Converter<?, ?, ?, ?, ?, ?, ?, ?> that = (Tuple4Converter<?, ?, ?, ?, ?, ?, ?, ?>)obj;
        return this.value1Converter.equals(that.value1Converter)
          && this.value2Converter.equals(that.value2Converter)
          && this.value3Converter.equals(that.value3Converter)
          && this.value4Converter.equals(that.value4Converter);
    }

    @Override
    public int hashCode() {
        return this.value1Converter.hashCode()
          ^ this.value2Converter.hashCode()
          ^ this.value3Converter.hashCode()
          ^ this.value4Converter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[value1Converter=" + this.value1Converter
          + ",value2Converter=" + this.value2Converter
          + ",value3Converter=" + this.value3Converter
          + ",value4Converter=" + this.value4Converter
          + "]";
    }
}
