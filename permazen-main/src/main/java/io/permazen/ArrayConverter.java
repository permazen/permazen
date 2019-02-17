
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.lang.reflect.Array;

class ArrayConverter<A, B> extends Converter<A[], B[]> {

    private final Converter<A, B> converter;
    private final Class<A> aType;
    private final Class<B> bType;

    @SuppressWarnings("unchecked")
    ArrayConverter(Class<A> aType, Class<B> bType, Converter<A, B> converter) {
        Preconditions.checkArgument(aType != null, "null aType");
        Preconditions.checkArgument(bType != null, "null bType");
        Preconditions.checkArgument(converter != null, "null converter");
        this.aType = aType;
        this.bType = bType;
        this.converter = converter;
    }

    @Override
    protected B[] doForward(A[] value) {
        return ArrayConverter.convert(this.aType, this.bType, this.converter, value);
    }

    @Override
    protected A[] doBackward(B[] value) {
        return ArrayConverter.convert(this.bType, this.aType, this.converter.reverse(), value);
    }

    @SuppressWarnings("unchecked")
    private static <X, Y> Y[] convert(Class<X> xType, Class<Y> yType, Converter<X, Y> converter, X[] value) {
        if (value == null)
            return null;
        final int length = Array.getLength(value);
        final Y[] result = (Y[])Array.newInstance(yType, length);
        for (int i = 0; i < length; i++)
            result[i] = converter.convert(value[i]);
        return result;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ArrayConverter<?, ?> that = (ArrayConverter<?, ?>)obj;
        return this.aType == that.aType
          && this.bType == that.bType
          && this.converter.equals(that.converter);
    }

    @Override
    public int hashCode() {
        return this.aType.hashCode()
          ^ this.bType.hashCode()
          ^ this.converter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.aType.getName() + "[]->" + this.bType.getName() + "[]]";
    }
}

