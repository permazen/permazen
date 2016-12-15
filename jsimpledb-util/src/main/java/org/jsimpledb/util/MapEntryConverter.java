
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;

import java.util.Map;

/**
 * Converts {@link Map.Entry}s.
 *
 * @param <K> key type of this map
 * @param <V> value type of this map
 * @param <WK> key type of wrapped map
 * @param <WV> value type of wrapped map
 */
class MapEntryConverter<K, V, WK, WV> extends Converter<Map.Entry<K, V>, Map.Entry<WK, WV>> {

    private final Converter<K, WK> keyConverter;
    private final Converter<V, WV> valueConverter;

    MapEntryConverter(Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    @Override
    protected Map.Entry<WK, WV> doForward(Map.Entry<K, V> entry) {
        if (entry == null)
            return null;
        return new ConvertedMapEntry<WK, WV, K, V>(this.keyConverter.reverse(), this.valueConverter.reverse(), entry);
    }

    @Override
    protected Map.Entry<K, V> doBackward(Map.Entry<WK, WV> wentry) {
        if (wentry == null)
            return null;
        return new ConvertedMapEntry<>(this.keyConverter, this.valueConverter, wentry);
    }
}

