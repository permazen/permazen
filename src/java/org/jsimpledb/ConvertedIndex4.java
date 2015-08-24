
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.index.Index;
import org.jsimpledb.index.Index2;
import org.jsimpledb.index.Index3;
import org.jsimpledb.index.Index4;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;
import org.jsimpledb.tuple.Tuple4;
import org.jsimpledb.tuple.Tuple5;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.ConvertedNavigableSet;

/**
 * Converter for {@link Index4}s.
 *
 * @param <V1> first value type of this index
 * @param <V2> second value type of this index
 * @param <V3> third value type of this index
 * @param <V4> fourth value type of this index
 * @param <T> target type of this index
 * @param <WV1> first value type of wrapped index
 * @param <WV2> second value type of wrapped index
 * @param <WV3> third value type of wrapped index
 * @param <WV4> fourth value type of wrapped index
 * @param <WT> target type of wrapped index
 */
class ConvertedIndex4<V1, V2, V3, V4, T, WV1, WV2, WV3, WV4, WT> implements Index4<V1, V2, V3, V4, T> {

    private final Index4<WV1, WV2, WV3, WV4, WT> index;
    private final Converter<V1, WV1> value1Converter;
    private final Converter<V2, WV2> value2Converter;
    private final Converter<V3, WV3> value3Converter;
    private final Converter<V4, WV4> value4Converter;
    private final Converter<T, WT> targetConverter;

    public ConvertedIndex4(Index4<WV1, WV2, WV3, WV4, WT> index, Converter<V1, WV1> value1Converter,
      Converter<V2, WV2> value2Converter, Converter<V3, WV3> value3Converter, Converter<V4, WV4> value4Converter,
      Converter<T, WT> targetConverter) {
        Preconditions.checkArgument(value1Converter != null, "null value1Converter");
        Preconditions.checkArgument(value2Converter != null, "null value2Converter");
        Preconditions.checkArgument(value3Converter != null, "null value3Converter");
        Preconditions.checkArgument(value4Converter != null, "null value4Converter");
        Preconditions.checkArgument(targetConverter != null, "null targetConverter");
        this.index = index;
        this.value1Converter = value1Converter;
        this.value2Converter = value2Converter;
        this.value3Converter = value3Converter;
        this.value4Converter = value4Converter;
        this.targetConverter = targetConverter;
    }

    @Override
    public NavigableSet<Tuple5<V1, V2, V3, V4, T>> asSet() {
        return new ConvertedNavigableSet<Tuple5<V1, V2, V3, V4, T>, Tuple5<WV1, WV2, WV3, WV4, WT>>(
          this.index.asSet(),
          new Tuple5Converter<V1, V2, V3, V4, T, WV1, WV2, WV3, WV4, WT>(this.value1Converter,
            this.value2Converter, this.value3Converter, this.value4Converter, this.targetConverter));
    }

    @Override
    public NavigableMap<Tuple4<V1, V2, V3, V4>, NavigableSet<T>> asMap() {
        return new ConvertedNavigableMap<Tuple4<V1, V2, V3, V4>, NavigableSet<T>, Tuple4<WV1, WV2, WV3, WV4>, NavigableSet<WT>>(
          this.index.asMap(),
          new Tuple4Converter<V1, V2, V3, V4, WV1, WV2, WV3, WV4>(this.value1Converter,
            this.value2Converter, this.value3Converter, this.value4Converter),
            new NavigableSetConverter<T, WT>(this.targetConverter));
    }

    @Override
    public NavigableMap<Tuple3<V1, V2, V3>, Index<V4, T>> asMapOfIndex() {
        return new ConvertedNavigableMap<Tuple3<V1, V2, V3>, Index<V4, T>, Tuple3<WV1, WV2, WV3>, Index<WV4, WT>>(
          this.index.asMapOfIndex(),
          new Tuple3Converter<V1, V2, V3, WV1, WV2, WV3>(this.value1Converter, this.value2Converter, this.value3Converter),
          new IndexConverter<V4, T, WV4, WT>(this.value4Converter, this.targetConverter));
    }

    @Override
    public NavigableMap<Tuple2<V1, V2>, Index2<V3, V4, T>> asMapOfIndex2() {
        return new ConvertedNavigableMap<Tuple2<V1, V2>, Index2<V3, V4, T>, Tuple2<WV1, WV2>, Index2<WV3, WV4, WT>>(
          this.index.asMapOfIndex2(),
          new Tuple2Converter<V1, V2, WV1, WV2>(this.value1Converter, this.value2Converter),
          new Index2Converter<V3, V4, T, WV3, WV4, WT>(this.value3Converter, this.value4Converter, this.targetConverter));
    }

    @Override
    public NavigableMap<V1, Index3<V2, V3, V4, T>> asMapOfIndex3() {
        return new ConvertedNavigableMap<V1, Index3<V2, V3, V4, T>, WV1, Index3<WV2, WV3, WV4, WT>>(
          this.index.asMapOfIndex3(),
          this.value1Converter,
          new Index3Converter<V2, V3, V4, T, WV2, WV3, WV4, WT>(this.value2Converter,
            this.value3Converter, this.value4Converter, this.targetConverter));
    }

    @Override
    public Index3<V1, V2, V3, V4> asIndex3() {
        return new ConvertedIndex3<V1, V2, V3, V4, WV1, WV2, WV3, WV4>(this.index.asIndex3(),
          this.value1Converter, this.value2Converter, this.value3Converter, this.value4Converter);
    }

    @Override
    public Index2<V1, V2, V3> asIndex2() {
        return new ConvertedIndex2<V1, V2, V3, WV1, WV2, WV3>(this.index.asIndex2(),
          this.value1Converter, this.value2Converter, this.value3Converter);
    }

    @Override
    public Index<V1, V2> asIndex() {
        return new ConvertedIndex<V1, V2, WV1, WV2>(this.index.asIndex(), this.value1Converter, this.value2Converter);
    }
}

