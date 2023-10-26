
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.tuple.Tuple5;

/**
 * Converts {@link Tuple5}s.
 *
 * @param <V1> first converted value
 * @param <V2> second converted value
 * @param <V3> third converted value
 * @param <V4> fouth converted value
 * @param <V5> fifth converted value
 * @param <W1> first wrapped value
 * @param <W2> second wrapped value
 * @param <W3> third wrapped value
 * @param <W4> fourth wrapped value
 * @param <W5> fifth wrapped value
 */
class Tuple5Converter<V1, V2, V3, V4, V5, W1, W2, W3, W4, W5>
  extends Converter<Tuple5<V1, V2, V3, V4, V5>, Tuple5<W1, W2, W3, W4, W5>> {

    private final Converter<V1, W1> value1Converter;
    private final Converter<V2, W2> value2Converter;
    private final Converter<V3, W3> value3Converter;
    private final Converter<V4, W4> value4Converter;
    private final Converter<V5, W5> value5Converter;

    Tuple5Converter(Converter<V1, W1> value1Converter, Converter<V2, W2> value2Converter,
      Converter<V3, W3> value3Converter, Converter<V4, W4> value4Converter, Converter<V5, W5> value5Converter) {
        Preconditions.checkArgument(value1Converter != null, "null value1Converter");
        Preconditions.checkArgument(value2Converter != null, "null value2Converter");
        Preconditions.checkArgument(value3Converter != null, "null value3Converter");
        Preconditions.checkArgument(value4Converter != null, "null value4Converter");
        Preconditions.checkArgument(value5Converter != null, "null value5Converter");
        this.value1Converter = value1Converter;
        this.value2Converter = value2Converter;
        this.value3Converter = value3Converter;
        this.value4Converter = value4Converter;
        this.value5Converter = value5Converter;
    }

    @Override
    protected Tuple5<W1, W2, W3, W4, W5> doForward(Tuple5<V1, V2, V3, V4, V5> tuple) {
        if (tuple == null)
            return null;
        return new Tuple5<W1, W2, W3, W4, W5>(
          this.value1Converter.convert(tuple.getValue1()),
          this.value2Converter.convert(tuple.getValue2()),
          this.value3Converter.convert(tuple.getValue3()),
          this.value4Converter.convert(tuple.getValue4()),
          this.value5Converter.convert(tuple.getValue5()));
    }

    @Override
    protected Tuple5<V1, V2, V3, V4, V5> doBackward(Tuple5<W1, W2, W3, W4, W5> tuple) {
        if (tuple == null)
            return null;
        return new Tuple5<V1, V2, V3, V4, V5>(
          this.value1Converter.reverse().convert(tuple.getValue1()),
          this.value2Converter.reverse().convert(tuple.getValue2()),
          this.value3Converter.reverse().convert(tuple.getValue3()),
          this.value4Converter.reverse().convert(tuple.getValue4()),
          this.value5Converter.reverse().convert(tuple.getValue5()));
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Tuple5Converter<?, ?, ?, ?, ?, ?, ?, ?, ?, ?> that = (Tuple5Converter<?, ?, ?, ?, ?, ?, ?, ?, ?, ?>)obj;
        return this.value1Converter.equals(that.value1Converter)
          && this.value2Converter.equals(that.value2Converter)
          && this.value3Converter.equals(that.value3Converter)
          && this.value4Converter.equals(that.value4Converter)
          && this.value5Converter.equals(that.value5Converter);
    }

    @Override
    public int hashCode() {
        return this.value1Converter.hashCode()
          ^ this.value2Converter.hashCode()
          ^ this.value3Converter.hashCode()
          ^ this.value4Converter.hashCode()
          ^ this.value5Converter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[value1Converter=" + this.value1Converter
          + ",value2Converter=" + this.value2Converter
          + ",value3Converter=" + this.value3Converter
          + ",value4Converter=" + this.value4Converter
          + ",value5Converter=" + this.value5Converter
          + "]";
    }
}
