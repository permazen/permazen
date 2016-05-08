
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import org.jsimpledb.tuple.Tuple2;

/**
 * Converts {@link Tuple2}s.
 *
 * @param <V1> first converted value
 * @param <V2> second converted value
 * @param <W1> first wrapped value
 * @param <W2> second wrapped value
 */
class Tuple2Converter<V1, V2, W1, W2> extends Converter<Tuple2<V1, V2>, Tuple2<W1, W2>> {

    private final Converter<V1, W1> value1Converter;
    private final Converter<V2, W2> value2Converter;

    Tuple2Converter(Converter<V1, W1> value1Converter, Converter<V2, W2> value2Converter) {
        Preconditions.checkArgument(value1Converter != null, "null value1Converter");
        Preconditions.checkArgument(value2Converter != null, "null value2Converter");
        this.value1Converter = value1Converter;
        this.value2Converter = value2Converter;
    }

    @Override
    protected Tuple2<W1, W2> doForward(Tuple2<V1, V2> tuple) {
        if (tuple == null)
            return null;
        return new Tuple2<W1, W2>(
          this.value1Converter.convert(tuple.getValue1()),
          this.value2Converter.convert(tuple.getValue2()));
    }

    @Override
    protected Tuple2<V1, V2> doBackward(Tuple2<W1, W2> tuple) {
        if (tuple == null)
            return null;
        return new Tuple2<V1, V2>(
          this.value1Converter.reverse().convert(tuple.getValue1()),
          this.value2Converter.reverse().convert(tuple.getValue2()));
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Tuple2Converter<?, ?, ?, ?> that = (Tuple2Converter<?, ?, ?, ?>)obj;
        return this.value1Converter.equals(that.value1Converter)
          && this.value2Converter.equals(that.value2Converter);
    }

    @Override
    public int hashCode() {
        return this.value1Converter.hashCode() ^ this.value2Converter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[value1Converter=" + this.value1Converter
          + ",value2Converter=" + this.value2Converter
          + "]";
    }
}

