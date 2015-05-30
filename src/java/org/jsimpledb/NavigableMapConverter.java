
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import java.util.NavigableMap;

import org.jsimpledb.util.ConvertedNavigableMap;

/**
 * Converts {@link NavigableMap}s into {@link ConvertedNavigableMap}s using the provided element {@link Converter}.
 *
 * @param <K> key type of converted maps
 * @param <V> value type of converted maps
 * @param <WK> key type of unconverted (wrapped) maps
 * @param <WV> value type of unconverted (wrapped) maps
 */
class NavigableMapConverter<K, V, WK, WV> extends Converter<NavigableMap<K, V>, NavigableMap<WK, WV>> {

    private final Converter<K, WK> keyConverter;
    private final Converter<V, WV> valueConverter;

    NavigableMapConverter(Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        if (keyConverter == null)
            throw new IllegalArgumentException("null keyConverter");
        if (valueConverter == null)
            throw new IllegalArgumentException("null valueConverter");
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    @Override
    protected NavigableMap<WK, WV> doForward(NavigableMap<K, V> map) {
        if (map == null)
            return null;
        return new ConvertedNavigableMap<WK, WV, K, V>(map, this.keyConverter.reverse(), this.valueConverter.reverse());
    }

    @Override
    protected NavigableMap<K, V> doBackward(NavigableMap<WK, WV> map) {
        if (map == null)
            return null;
        return new ConvertedNavigableMap<K, V, WK, WV>(map, this.keyConverter, this.valueConverter);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final NavigableMapConverter<?, ?, ?, ?> that = (NavigableMapConverter<?, ?, ?, ?>)obj;
        return this.keyConverter.equals(that.keyConverter) && this.valueConverter.equals(that.valueConverter);
    }

    @Override
    public int hashCode() {
        return this.keyConverter.hashCode() ^ this.valueConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[keyConverter=" + this.keyConverter
          + ",valueConverter=" + this.valueConverter + "]";
    }
}

