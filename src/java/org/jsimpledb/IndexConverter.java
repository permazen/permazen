
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import org.jsimpledb.index.Index;

/**
 * Converts {@link Index}es.
 *
 * @param <V> value type of converted indexes
 * @param <T> target type of converted indexes
 * @param <WV> value type of unconverted (wrapped) indexes
 * @param <WT> target type of unconverted (wrapped) indexes
 */
class IndexConverter<V, T, WV, WT> extends Converter<Index<V, T>, Index<WV, WT>> {

    private final Converter<V, WV> valueConverter;
    private final Converter<T, WT> targetConverter;

    IndexConverter(Converter<V, WV> valueConverter, Converter<T, WT> targetConverter) {
        Preconditions.checkArgument(valueConverter != null, "null valueConverter");
        Preconditions.checkArgument(targetConverter != null, "null targetConverter");
        this.valueConverter = valueConverter;
        this.targetConverter = targetConverter;
    }

    @Override
    protected Index<WV, WT> doForward(Index<V, T> index) {
        if (index == null)
            return null;
        return new ConvertedIndex<WV, WT, V, T>(index, this.valueConverter.reverse(), this.targetConverter.reverse());
    }

    @Override
    protected Index<V, T> doBackward(Index<WV, WT> index) {
        if (index == null)
            return null;
        return new ConvertedIndex<V, T, WV, WT>(index, this.valueConverter, this.targetConverter);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final IndexConverter<?, ?, ?, ?> that = (IndexConverter<?, ?, ?, ?>)obj;
        return this.valueConverter.equals(that.valueConverter) && this.targetConverter.equals(that.targetConverter);
    }

    @Override
    public int hashCode() {
        return this.valueConverter.hashCode() ^ this.targetConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[valueConverter=" + this.valueConverter
          + ",targetConverter=" + this.targetConverter + "]";
    }
}

