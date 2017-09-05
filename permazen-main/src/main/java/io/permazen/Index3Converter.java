
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.index.Index3;

/**
 * Converts {@link Index3}es.
 *
 * @param <V1> first value type of converted indexes
 * @param <V2> second value type of converted indexes
 * @param <V3> third value type of converted indexes
 * @param <T> target type of converted indexes
 * @param <WV1> first value type of unconverted (wrapped) indexes
 * @param <WV2> second value type of unconverted (wrapped) indexes
 * @param <WV3> third value type of unconverted (wrapped) indexes
 * @param <WT> target type of unconverted (wrapped) indexes
 */
class Index3Converter<V1, V2, V3, T, WV1, WV2, WV3, WT> extends Converter<Index3<V1, V2, V3, T>, Index3<WV1, WV2, WV3, WT>> {

    private final Converter<V1, WV1> value1Converter;
    private final Converter<V2, WV2> value2Converter;
    private final Converter<V3, WV3> value3Converter;
    private final Converter<T, WT> targetConverter;

    Index3Converter(Converter<V1, WV1> value1Converter, Converter<V2, WV2> value2Converter,
      Converter<V3, WV3> value3Converter, Converter<T, WT> targetConverter) {
        Preconditions.checkArgument(value1Converter != null, "null value1Converter");
        Preconditions.checkArgument(value2Converter != null, "null value2Converter");
        Preconditions.checkArgument(value3Converter != null, "null value3Converter");
        Preconditions.checkArgument(targetConverter != null, "null targetConverter");
        this.value1Converter = value1Converter;
        this.value2Converter = value2Converter;
        this.value3Converter = value3Converter;
        this.targetConverter = targetConverter;
    }

    @Override
    protected Index3<WV1, WV2, WV3, WT> doForward(Index3<V1, V2, V3, T> index) {
        if (index == null)
            return null;
        return new ConvertedIndex3<WV1, WV2, WV3, WT, V1, V2, V3, T>(index,
          this.value1Converter.reverse(), this.value2Converter.reverse(),
          this.value3Converter.reverse(), this.targetConverter.reverse());
    }

    @Override
    protected Index3<V1, V2, V3, T> doBackward(Index3<WV1, WV2, WV3, WT> index) {
        if (index == null)
            return null;
        return new ConvertedIndex3<>(index,
          this.value1Converter, this.value2Converter, this.value3Converter, this.targetConverter);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Index3Converter<?, ?, ?, ?, ?, ?, ?, ?> that = (Index3Converter<?, ?, ?, ?, ?, ?, ?, ?>)obj;
        return this.value1Converter.equals(that.value1Converter)
          && this.value2Converter.equals(that.value2Converter)
          && this.value3Converter.equals(that.value3Converter)
          && this.targetConverter.equals(that.targetConverter);
    }

    @Override
    public int hashCode() {
        return this.value1Converter.hashCode()
          ^ this.value2Converter.hashCode()
          ^ this.value3Converter.hashCode()
          ^ this.targetConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[value1Converter=" + this.value1Converter
          + ",value2Converter=" + this.value2Converter
          + ",value3Converter=" + this.value3Converter
          + ",targetConverter=" + this.targetConverter
          + "]";
    }
}

