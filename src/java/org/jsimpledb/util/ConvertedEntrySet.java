
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;

import java.util.Map;

/**
 * Converts a map's entry set.
 *
 * @param <K> key type of this map
 * @param <V> value type of this map
 * @param <WK> key type of wrapped map
 * @param <WV> value type of wrapped map
 */
class ConvertedEntrySet<K, V, WK, WV> extends ConvertedSet<Map.Entry<K, V>, Map.Entry<WK, WV>> {

    private final Map<WK, WV> map;
    private final Converter<K, WK> keyConverter;
    private final Converter<V, WV> valueConverter;

    /**
     * Constructor.
     *
     * @param set wrapped set
     * @param converter element converter
     * @throws IllegalArgumentException if any parameter is null
     */
    ConvertedEntrySet(Map<WK, WV> map, Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        super(map.entrySet(), new MapEntryConverter<K, V, WK, WV>(keyConverter, valueConverter));
        if (keyConverter == null)
            throw new IllegalArgumentException("null keyConverter");
        if (valueConverter == null)
            throw new IllegalArgumentException("null valueConverter");
        this.map = map;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    public Converter<K, WK> getKeyConverter() {
        return this.keyConverter;
    }

    public Converter<V, WV> getValueConverter() {
        return this.valueConverter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object obj) {

        // Check type
        if (!(obj instanceof Map.Entry))
            return false;
        final Map.Entry<?, ?> entry = (Map.Entry<?, ?>)obj;

        // Convert key to wrapped key
        final K key = (K)entry.getKey();
        WK wkey = null;
        if (key != null) {
            try {
                wkey = this.keyConverter.convert(key);
            } catch (ClassCastException e) {
                return false;
            }
        }

        // Get corresponding wrapped value, if any, and unwrap it
        final WV wvalue = this.map.get(wkey);
        if (wvalue == null && !this.map.containsKey(wkey))
            return false;
        final V value = this.valueConverter.reverse().convert(wvalue);

        // Compare original value to unwrapped value
        return value != null ? value.equals(entry.getValue()) : entry.getValue() == null;
    }

    @Override
    public boolean add(Map.Entry<K, V> entry) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object obj) {

        // Check type
        if (!(obj instanceof Map.Entry))
            return false;
        final Map.Entry<?, ?> entry = (Map.Entry<?, ?>)obj;

        // Convert key to wrapped key
        final K key = (K)entry.getKey();
        WK wkey = null;
        if (key != null) {
            try {
                wkey = this.keyConverter.convert(key);
            } catch (ClassCastException e) {
                return false;
            }
        }

        // Get corresponding wrapped value, if any, and unwrap it
        final WV wvalue = this.map.get(wkey);
        if (wvalue == null && !this.map.containsKey(wkey))
            return false;
        final V value = this.valueConverter.reverse().convert(wvalue);

        // Compare original value to unwrapped value and remove entry if it matches
        if (value != null ? value.equals(entry.getValue()) : entry.getValue() == null) {
            this.map.remove(wkey);
            return true;
        }

        // Not found
        return false;
    }
}

