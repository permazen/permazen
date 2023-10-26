
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.index.Index;
import io.permazen.tuple.Tuple2;
import io.permazen.util.Bounds;
import io.permazen.util.ConvertedNavigableMap;
import io.permazen.util.ConvertedNavigableSet;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Converter for {@link Index}es.
 *
 * @param <V> value type of this index
 * @param <T> target type of this index
 * @param <WV> value type of wrapped index
 * @param <WT> target type of wrapped index
 */
class ConvertedIndex<V, T, WV, WT> implements Index<V, T> {

    private final Index<WV, WT> index;
    private final Converter<V, WV> valueConverter;
    private final Converter<T, WT> targetConverter;

    ConvertedIndex(Index<WV, WT> index, Converter<V, WV> valueConverter, Converter<T, WT> targetConverter) {
        Preconditions.checkArgument(index != null, "null index");
        Preconditions.checkArgument(valueConverter != null, "null valueConverter");
        Preconditions.checkArgument(targetConverter != null, "null targetConverter");
        this.index = index;
        this.valueConverter = valueConverter;
        this.targetConverter = targetConverter;
    }

    @Override
    public NavigableSet<Tuple2<V, T>> asSet() {
        return new ConvertedNavigableSet<Tuple2<V, T>, Tuple2<WV, WT>>(this.index.asSet(),
          new Tuple2Converter<V, T, WV, WT>(this.valueConverter, this.targetConverter));
    }

    @Override
    public NavigableMap<V, NavigableSet<T>> asMap() {
        return new ConvertedNavigableMap<V, NavigableSet<T>, WV, NavigableSet<WT>>(this.index.asMap(),
          this.valueConverter, new NavigableSetConverter<T, WT>(this.targetConverter));
    }

    @Override
    public Index<V, T> withValueBounds(Bounds<V> bounds) {
        return this.convert(this.index.withValueBounds(ConvertedIndex.convert(bounds, this.valueConverter)));
    }

    @Override
    public Index<V, T> withTargetBounds(Bounds<T> bounds) {
        return this.convert(this.index.withTargetBounds(ConvertedIndex.convert(bounds, this.targetConverter)));
    }

    private Index<V, T> convert(Index<WV, WT> boundedIndex) {
        return boundedIndex == this.index ? this :
          new ConvertedIndex<V, T, WV, WT>(boundedIndex, this.valueConverter, this.targetConverter);
    }

    static <T1, T2> Bounds<T2> convert(Bounds<T1> bounds, Converter<T1, T2> converter) {
        final T2 lowerBound = bounds.hasLowerBound() ? converter.convert(bounds.getLowerBound()) : null;
        final T2 upperBound = bounds.hasUpperBound() ? converter.convert(bounds.getUpperBound()) : null;
        return new Bounds<T2>(lowerBound, bounds.getLowerBoundType(), upperBound, bounds.getUpperBoundType());
    }
}
