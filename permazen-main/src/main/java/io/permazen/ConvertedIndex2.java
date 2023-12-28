
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.index.Index1;
import io.permazen.index.Index2;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.util.Bounds;
import io.permazen.util.ConvertedNavigableMap;
import io.permazen.util.ConvertedNavigableSet;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Converter for {@link Index2}s.
 *
 * @param <V1> first value type of this index
 * @param <V2> second value type of this index
 * @param <T> target type of this index
 * @param <WV1> first value type of wrapped index
 * @param <WV2> second value type of wrapped index
 * @param <WT> target type of wrapped index
 */
class ConvertedIndex2<V1, V2, T, WV1, WV2, WT> implements Index2<V1, V2, T> {

    private final Index2<WV1, WV2, WT> index;
    private final Converter<V1, WV1> value1Converter;
    private final Converter<V2, WV2> value2Converter;
    private final Converter<T, WT> targetConverter;

    ConvertedIndex2(Index2<WV1, WV2, WT> index,
      Converter<V1, WV1> value1Converter, Converter<V2, WV2> value2Converter, Converter<T, WT> targetConverter) {
        Preconditions.checkArgument(index != null, "null index");
        Preconditions.checkArgument(value1Converter != null, "null value1Converter");
        Preconditions.checkArgument(value2Converter != null, "null value2Converter");
        Preconditions.checkArgument(targetConverter != null, "null targetConverter");
        this.index = index;
        this.value1Converter = value1Converter;
        this.value2Converter = value2Converter;
        this.targetConverter = targetConverter;
    }

    @Override
    public NavigableSet<Tuple3<V1, V2, T>> asSet() {
        return new ConvertedNavigableSet<Tuple3<V1, V2, T>, Tuple3<WV1, WV2, WT>>(this.index.asSet(),
          new Tuple3Converter<V1, V2, T, WV1, WV2, WT>(this.value1Converter, this.value2Converter, this.targetConverter));
    }

    @Override
    public NavigableMap<Tuple2<V1, V2>, NavigableSet<T>> asMap() {
        return new ConvertedNavigableMap<Tuple2<V1, V2>, NavigableSet<T>, Tuple2<WV1, WV2>, NavigableSet<WT>>(this.index.asMap(),
          new Tuple2Converter<V1, V2, WV1, WV2>(this.value1Converter, this.value2Converter),
          new NavigableSetConverter<T, WT>(this.targetConverter));
    }

    @Override
    public NavigableMap<V1, Index1<V2, T>> asMapOfIndex1() {
        return new ConvertedNavigableMap<V1, Index1<V2, T>, WV1, Index1<WV2, WT>>(this.index.asMapOfIndex1(),
          this.value1Converter, new Index1Converter<V2, T, WV2, WT>(this.value2Converter, this.targetConverter));
    }

    @Override
    public Index1<V1, V2> asIndex1() {
        return new ConvertedIndex1<>(this.index.asIndex1(), this.value1Converter, this.value2Converter);
    }

    @Override
    public Index2<V1, V2, T> withValue1Bounds(Bounds<V1> bounds) {
        return this.convert(this.index.withValue1Bounds(ConvertedIndex1.convert(bounds, this.value1Converter)));
    }

    @Override
    public Index2<V1, V2, T> withValue2Bounds(Bounds<V2> bounds) {
        return this.convert(this.index.withValue2Bounds(ConvertedIndex1.convert(bounds, this.value2Converter)));
    }

    @Override
    public Index2<V1, V2, T> withTargetBounds(Bounds<T> bounds) {
        return this.convert(this.index.withTargetBounds(ConvertedIndex1.convert(bounds, this.targetConverter)));
    }

    private Index2<V1, V2, T> convert(Index2<WV1, WV2, WT> boundedIndex) {
        return boundedIndex == this.index ? this :
          new ConvertedIndex2<V1, V2, T, WV1, WV2, WT>(boundedIndex,
           this.value1Converter, this.value2Converter, this.targetConverter);
    }
}
