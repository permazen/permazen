
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.index.Index;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.ConvertedNavigableSet;

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

    public ConvertedIndex(Index<WV, WT> index, Converter<V, WV> valueConverter, Converter<T, WT> targetConverter) {
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
}

