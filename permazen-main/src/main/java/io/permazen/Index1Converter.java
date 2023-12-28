
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.index.Index1;

/**
 * Converts {@link Index1}es.
 *
 * @param <V> value type of converted indexes
 * @param <T> target type of converted indexes
 * @param <WV> value type of unconverted (wrapped) indexes
 * @param <WT> target type of unconverted (wrapped) indexes
 */
class Index1Converter<V, T, WV, WT> extends Converter<Index1<V, T>, Index1<WV, WT>> {

    private final Converter<V, WV> valueConverter;
    private final Converter<T, WT> targetConverter;

    Index1Converter(Converter<V, WV> valueConverter, Converter<T, WT> targetConverter) {
        Preconditions.checkArgument(valueConverter != null, "null valueConverter");
        Preconditions.checkArgument(targetConverter != null, "null targetConverter");
        this.valueConverter = valueConverter;
        this.targetConverter = targetConverter;
    }

    @Override
    protected Index1<WV, WT> doForward(Index1<V, T> index) {
        if (index == null)
            return null;
        return new ConvertedIndex1<WV, WT, V, T>(index, this.valueConverter.reverse(), this.targetConverter.reverse());
    }

    @Override
    protected Index1<V, T> doBackward(Index1<WV, WT> index) {
        if (index == null)
            return null;
        return new ConvertedIndex1<>(index, this.valueConverter, this.targetConverter);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Index1Converter<?, ?, ?, ?> that = (Index1Converter<?, ?, ?, ?>)obj;
        return this.valueConverter.equals(that.valueConverter) && this.targetConverter.equals(that.targetConverter);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ this.valueConverter.hashCode()
          ^ this.targetConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[valueConverter=" + this.valueConverter
          + ",targetConverter=" + this.targetConverter + "]";
    }
}
