
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import org.jsimpledb.index.Index2;

/**
 * Converts {@link Index2}es.
 *
 * @param <V1> first value type of converted indexes
 * @param <V2> second value type of converted indexes
 * @param <T> target type of converted indexes
 * @param <WV1> first value type of unconverted (wrapped) indexes
 * @param <WV2> second value type of unconverted (wrapped) indexes
 * @param <WT> target type of unconverted (wrapped) indexes
 */
class Index2Converter<V1, V2, T, WV1, WV2, WT> extends Converter<Index2<V1, V2, T>, Index2<WV1, WV2, WT>> {

    private final Converter<V1, WV1> value1Converter;
    private final Converter<V2, WV2> value2Converter;
    private final Converter<T, WT> targetConverter;

    Index2Converter(Converter<V1, WV1> value1Converter, Converter<V2, WV2> value2Converter, Converter<T, WT> targetConverter) {
        Preconditions.checkArgument(value1Converter != null, "null value1Converter");
        Preconditions.checkArgument(value2Converter != null, "null value2Converter");
        Preconditions.checkArgument(targetConverter != null, "null targetConverter");
        this.value1Converter = value1Converter;
        this.value2Converter = value2Converter;
        this.targetConverter = targetConverter;
    }

    @Override
    protected Index2<WV1, WV2, WT> doForward(Index2<V1, V2, T> index) {
        if (index == null)
            return null;
        return new ConvertedIndex2<WV1, WV2, WT, V1, V2, T>(index,
          this.value1Converter.reverse(), this.value2Converter.reverse(), this.targetConverter.reverse());
    }

    @Override
    protected Index2<V1, V2, T> doBackward(Index2<WV1, WV2, WT> index) {
        if (index == null)
            return null;
        return new ConvertedIndex2<>(index,
          this.value1Converter, this.value2Converter, this.targetConverter);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Index2Converter<?, ?, ?, ?, ?, ?> that = (Index2Converter<?, ?, ?, ?, ?, ?>)obj;
        return this.value1Converter.equals(that.value1Converter)
          && this.value2Converter.equals(that.value2Converter)
          && this.targetConverter.equals(that.targetConverter);
    }

    @Override
    public int hashCode() {
        return this.value1Converter.hashCode()
          ^ this.value2Converter.hashCode()
          ^ this.targetConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[value1Converter=" + this.value1Converter
          + ",value2Converter=" + this.value2Converter
          + ",targetConverter=" + this.targetConverter
          + "]";
    }
}

