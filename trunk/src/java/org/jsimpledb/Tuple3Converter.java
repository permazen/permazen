
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import org.jsimpledb.tuple.Tuple3;

/**
 * Converts {@link Tuple3}s.
 *
 * @param <V1> first converted value
 * @param <V2> second converted value
 * @param <V3> third converted value
 * @param <W1> first wrapped value
 * @param <W2> second wrapped value
 * @param <W3> third wrapped value
 */
class Tuple3Converter<V1, V2, V3, W1, W2, W3> extends Converter<Tuple3<V1, V2, V3>, Tuple3<W1, W2, W3>> {

    private final Converter<V1, W1> value1Converter;
    private final Converter<V2, W2> value2Converter;
    private final Converter<V3, W3> value3Converter;

    public Tuple3Converter(Converter<V1, W1> value1Converter,
      Converter<V2, W2> value2Converter, Converter<V3, W3> value3Converter) {
        if (value1Converter == null)
            throw new IllegalArgumentException("null value1Converter");
        if (value2Converter == null)
            throw new IllegalArgumentException("null value2Converter");
        if (value3Converter == null)
            throw new IllegalArgumentException("null value3Converter");
        this.value1Converter = value1Converter;
        this.value2Converter = value2Converter;
        this.value3Converter = value3Converter;
    }

    @Override
    protected Tuple3<W1, W2, W3> doForward(Tuple3<V1, V2, V3> tuple) {
        if (tuple == null)
            return null;
        return new Tuple3<W1, W2, W3>(
          this.value1Converter.convert(tuple.getValue1()),
          this.value2Converter.convert(tuple.getValue2()),
          this.value3Converter.convert(tuple.getValue3()));
    }

    @Override
    protected Tuple3<V1, V2, V3> doBackward(Tuple3<W1, W2, W3> tuple) {
        if (tuple == null)
            return null;
        return new Tuple3<V1, V2, V3>(
          this.value1Converter.reverse().convert(tuple.getValue1()),
          this.value2Converter.reverse().convert(tuple.getValue2()),
          this.value3Converter.reverse().convert(tuple.getValue3()));
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Tuple3Converter<?, ?, ?, ?, ?, ?> that = (Tuple3Converter<?, ?, ?, ?, ?, ?>)obj;
        return this.value1Converter.equals(that.value1Converter)
          && this.value2Converter.equals(that.value2Converter)
          && this.value3Converter.equals(that.value3Converter);
    }

    @Override
    public int hashCode() {
        return this.value1Converter.hashCode() ^ this.value2Converter.hashCode() ^ this.value3Converter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[value1Converter=" + this.value1Converter
          + ",value2Converter=" + this.value2Converter
          + ",value3Converter=" + this.value3Converter
          + "]";
    }
}

