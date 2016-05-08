
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;

import java.util.AbstractMap;
import java.util.Map;

/**
 * Converted {@link Map.Entry}.
 *
 * @param <K> key type of this entry
 * @param <V> value type of this entry
 * @param <WK> key type of wrapped entry
 * @param <WV> value type of wrapped entry
 */
@SuppressWarnings("serial")
class ConvertedMapEntry<K, V, WK, WV> extends AbstractMap.SimpleEntry<K, V> {

    private final Converter<K, WK> keyConverter;
    private final Converter<V, WV> valueConverter;
    private final Map.Entry<WK, WV> wentry;

    ConvertedMapEntry(Converter<K, WK> keyConverter, Converter<V, WV> valueConverter, Map.Entry<WK, WV> wentry) {
        super(wentry.getKey() != null ? keyConverter.reverse().convert(wentry.getKey()) : null,
          wentry.getValue() != null ? valueConverter.reverse().convert(wentry.getValue()) : null);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.wentry = wentry;
    }

    public Converter<K, WK> getKeyConverter() {
        return this.keyConverter;
    }

    public Converter<V, WV> getValueConverter() {
        return this.valueConverter;
    }

    @Override
    public K getKey() {
        final WK key = this.wentry.getKey();
        return key != null ? this.keyConverter.reverse().convert(key) : null;
    }

    @Override
    public V getValue() {
        final WV value = this.wentry.getValue();
        return value != null ? this.valueConverter.reverse().convert(value) : null;
    }

    @Override
    public V setValue(V value) {
        final WV prev = this.wentry.setValue(value != null ? this.valueConverter.convert(value) : null);
        return prev != null ? this.valueConverter.reverse().convert(prev) : null;
    }
}

