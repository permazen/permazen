
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Provides a transformed view of a wrapped {@link NavigableMap} using a strictly invertable {@link Converter}.
 *
 * @param <K> key type of this map
 * @param <V> value type of this map
 * @param <WK> key type of wrapped map
 * @param <WV> value type of wrapped map
 */
public class ConvertedNavigableMap<K, V, WK, WV> extends AbstractNavigableMap<K, V> {

    private final NavigableMap<WK, WV> map;
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
    public ConvertedNavigableMap(NavigableMap<WK, WV> map, Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        this(map, keyConverter, valueConverter, new Bounds<K>());
    }

    /**
     * Internal constructor.
     *
     * @param map wrapped map
     * @param keyConverter key converter
     * @param valueConverter value converter
     * @throws IllegalArgumentException if any parameter is null
     */
    ConvertedNavigableMap(NavigableMap<WK, WV> map,
      Converter<K, WK> keyConverter, Converter<V, WV> valueConverter, Bounds<K> bounds) {
        super(bounds);
        Preconditions.checkArgument(map != null, "null map");
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
    public Comparator<? super K> comparator() {
        return new ConvertedComparator<K, WK>(this.map.comparator(), this.keyConverter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        WK wkey = null;
        if (key != null) {
            try {
                wkey = this.keyConverter.convert((K)key);
            } catch (ClassCastException e) {
                return null;
            }
        }
        final WV wvalue = this.map.get(wkey);
        return wvalue != null ? this.valueConverter.reverse().convert(wvalue) : null;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new ConvertedEntrySet<K, V, WK, WV>(this.map, this.keyConverter, this.valueConverter);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return new ConvertedNavigableSet<K, WK>(this.map.navigableKeySet(), this.keyConverter);
    }

    @Override
    public V put(K key, V value) {
        final WK wkey = key != null ? this.keyConverter.convert(key) : null;
        final WV wvalue = value != null ? this.valueConverter.convert(value) : null;
        final WV wprev = this.map.put(wkey, wvalue);
        return wprev != null ? this.valueConverter.reverse().convert(wprev) : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        WK wkey = null;
        if (key != null) {
            try {
                wkey = this.keyConverter.convert((K)key);
            } catch (ClassCastException e) {
                return null;
            }
        }
        final WV wvalue = this.map.remove(wkey);
        return wvalue != null ? this.valueConverter.reverse().convert(wvalue) : null;
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    protected Map.Entry<K, V> searchBelow(K maxKey, boolean inclusive) {
        try {
            return super.searchBelow(maxKey, inclusive);
        } catch (IllegalArgumentException e) {                      // handle case where elem is out of bounds
            final Map.Entry<K, V> lastEntry;
            try {
                lastEntry = this.lastEntry();
            } catch (NoSuchElementException e2) {
                return null;
            }
            return this.getComparator(false).compare(maxKey, lastEntry.getKey()) > 0 ? lastEntry : null;
        }
    }

    @Override
    protected Map.Entry<K, V> searchAbove(K minKey, boolean inclusive) {
        try {
            return super.searchAbove(minKey, inclusive);
        } catch (IllegalArgumentException e) {                      // handle case where elem is out of bounds
            final Map.Entry<K, V> firstEntry;
            try {
                firstEntry = this.firstEntry();
            } catch (NoSuchElementException e2) {
                return null;
            }
            return this.getComparator(false).compare(minKey, firstEntry.getKey()) < 0 ? firstEntry : null;
        }
    }

    @Override
    protected NavigableMap<K, V> createSubMap(boolean reverse, Bounds<K> newBounds) {
        final K lower = newBounds.getLowerBound();
        final K upper = newBounds.getUpperBound();
        final WK wlower = newBounds.getLowerBoundType() != BoundType.NONE && lower != null ?
          this.keyConverter.convert(lower) : null;
        final WK wupper = newBounds.getUpperBoundType() != BoundType.NONE && upper != null ?
          this.keyConverter.convert(upper) : null;
        NavigableMap<WK, WV> subMap = reverse ? this.map.descendingMap() : this.map;
        if (newBounds.getLowerBoundType() != BoundType.NONE && newBounds.getUpperBoundType() != BoundType.NONE) {
            subMap = subMap.subMap(
              wlower, newBounds.getLowerBoundType().isInclusive(),
              wupper, newBounds.getUpperBoundType().isInclusive());
        } else if (newBounds.getLowerBoundType() != BoundType.NONE)
            subMap = subMap.tailMap(wlower, newBounds.getLowerBoundType().isInclusive());
        else if (newBounds.getUpperBoundType() != BoundType.NONE)
            subMap = subMap.headMap(wupper, newBounds.getUpperBoundType().isInclusive());
        return new ConvertedNavigableMap<K, V, WK, WV>(subMap, this.keyConverter, this.valueConverter, newBounds);
    }
}

