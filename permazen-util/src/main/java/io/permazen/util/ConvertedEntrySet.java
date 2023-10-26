
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.Objects;

/**
 * Converts a map's entry set.
 *
 * <p>
 * Supplied {@link Converter}s may throw {@link ClassCastException} or {@link IllegalArgumentException}
 * if given an objects whose runtime type does not match the expected type.
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
     * @param map wrapped map
     * @param keyConverter key converter
     * @param valueConverter value converter
     * @throws IllegalArgumentException if any parameter is null
     */
    ConvertedEntrySet(Map<WK, WV> map, Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        super(map.entrySet(), new MapEntryConverter<K, V, WK, WV>(keyConverter, valueConverter));
        Preconditions.checkArgument(keyConverter != null, "null keyConverter");
        Preconditions.checkArgument(valueConverter != null, "null valueConverter");
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
            } catch (IllegalArgumentException | ClassCastException e) {
                return false;
            }
        }

        // Get corresponding wrapped value, if any, and unwrap it
        final WV wvalue = this.map.get(wkey);
        if (wvalue == null && !this.map.containsKey(wkey))
            return false;
        final V value;
        try {
            value = this.valueConverter.reverse().convert(wvalue);
        } catch (IllegalArgumentException | ClassCastException e) {
            return false;
        }

        // Compare original value to unwrapped value
        return Objects.equals(value, entry.getValue());
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
            } catch (IllegalArgumentException | ClassCastException e) {
                return false;
            }
        }

        // Get corresponding wrapped value, if any, and unwrap it
        final WV wvalue = this.map.get(wkey);
        if (wvalue == null && !this.map.containsKey(wkey))
            return false;
        final V value;
        try {
            value = this.valueConverter.reverse().convert(wvalue);
        } catch (IllegalArgumentException | ClassCastException e) {
            return false;
        }

        // Compare original value to unwrapped value and remove entry if it matches
        if (Objects.equals(value, entry.getValue())) {
            this.map.remove(wkey);
            return true;
        }

        // Not found
        return false;
    }
}
