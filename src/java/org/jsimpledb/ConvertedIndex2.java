
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.index.Index;
import org.jsimpledb.index.Index2;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.ConvertedNavigableSet;

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

    public ConvertedIndex2(Index2<WV1, WV2, WT> index,
      Converter<V1, WV1> value1Converter, Converter<V2, WV2> value2Converter, Converter<T, WT> targetConverter) {
        if (index == null)
            throw new IllegalArgumentException("null index");
        if (value1Converter == null)
            throw new IllegalArgumentException("null value1Converter");
        if (value2Converter == null)
            throw new IllegalArgumentException("null value2Converter");
        if (targetConverter == null)
            throw new IllegalArgumentException("null targetConverter");
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
    public NavigableMap<V1, Index<V2, T>> asMapOfIndex() {
        return new ConvertedNavigableMap<V1, Index<V2, T>, WV1, Index<WV2, WT>>(this.index.asMapOfIndex(),
          this.value1Converter, new IndexConverter<V2, T, WV2, WT>(this.value2Converter, this.targetConverter));
    }

    @Override
    public Index<V1, V2> asIndex() {
        return new ConvertedIndex<V1, V2, WV1, WV2>(this.index.asIndex(), this.value1Converter, this.value2Converter);
    }
}

