
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Preconditions;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An immutable {@link NavigableMap} implementation optimized for read efficiency.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("serial")
public class ImmutableNavigableMap<K, V> extends AbstractNavigableMap<K, V> {

    private final K[] keys;
    private final V[] vals;
    private final int minIndex;
    private final int maxIndex;
    private final Comparator<? super K> comparator;
    private final Comparator<? super K> actualComparator;

    /**
     * Constructor.
     *
     * @param source data source
     * @throws IllegalArgumentException if {@code source} is null
     */
    @SuppressWarnings("unchecked")
    public ImmutableNavigableMap(NavigableMap<K, V> source) {
        this((K[])source.keySet().toArray(), (V[])source.values().toArray(), source.comparator());
    }

    /**
     * Constructor.
     *
     * @param keys sorted key array
     * @param vals value array corresponding to {@code keys}
     * @param comparator key comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code keys} or {@code vals} is null
     * @throws IllegalArgumentException if {@code keys} and {@code vals} have different lengths
     */
    public ImmutableNavigableMap(K[] keys, V[] vals, Comparator<? super K> comparator) {
        this(new Bounds<>(), keys, vals, comparator);
    }

    private ImmutableNavigableMap(Bounds<K> bounds, K[] keys, V[] vals, Comparator<? super K> comparator) {
        this(bounds, keys, vals, 0, -1, comparator);
    }

    @SuppressWarnings("unchecked")
    private ImmutableNavigableMap(Bounds<K> bounds, K[] keys, V[] vals,
      int minIndex, int maxIndex, Comparator<? super K> comparator) {
        super(bounds);
        Preconditions.checkArgument(keys != null);
        Preconditions.checkArgument(vals != null);
        Preconditions.checkArgument(keys.length == vals.length);
        this.keys = keys;
        this.vals = vals;
        if (maxIndex == -1)
            maxIndex = this.keys.length;
        assert minIndex >= 0;
        assert maxIndex <= this.keys.length;
        assert minIndex <= maxIndex;
        this.minIndex = minIndex;
        this.maxIndex = maxIndex;
        this.comparator = comparator;
        this.actualComparator = this.comparator != null ? this.comparator : (Comparator<K>)Comparator.naturalOrder();
        for (int i = 1; i < this.keys.length; i++)
            assert this.actualComparator.compare(this.keys[i - 1], this.keys[i]) < 0;
    }

// NavigableSet

    @Override
    public Comparator<? super K> comparator() {
        return this.comparator;
    }

    @Override
    public boolean isEmpty() {
        return this.minIndex == this.maxIndex;
    }

    @Override
    public int size() {
        return this.maxIndex - this.minIndex;
    }

    @Override
    public boolean containsKey(Object obj) {
        return this.find(obj) >= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object obj) {
        final int index = this.find(obj);
        return index >= 0 ? this.vals[index] : null;
    }

    @Override
    public List<V> values() {
        List<V> list = Arrays.asList(this.vals);
        if (this.minIndex > 0 || this.maxIndex < this.vals.length)
            list = list.subList(this.minIndex, this.maxIndex);
        return Collections.unmodifiableList(list);
    }

    @Override
    public boolean containsValue(Object obj) {
        for (int i = this.minIndex; i < this.maxIndex; i++) {
            if (Objects.equals(obj, this.vals[i]))
                return true;
        }
        return false;
    }

    @Override
    public K firstKey() {
        return this.keys[this.checkIndex(this.minIndex)];
    }

    @Override
    public Map.Entry<K, V> firstEntry() {
        return !this.isEmpty() ? this.createEntry(this.minIndex) : null;
    }

    @Override
    public K lastKey() {
        return this.keys[this.checkIndex(this.maxIndex - 1)];
    }

    @Override
    public Map.Entry<K, V> lastEntry() {
        return !this.isEmpty() ? this.createEntry(this.maxIndex - 1) : null;
    }

    @Override
    public K lowerKey(K maxKey) {
        return this.findKey(maxKey, -1, -1);
    }

    @Override
    public Map.Entry<K, V> lowerEntry(K maxKey) {
        return this.findEntry(maxKey, -1, -1);
    }

    @Override
    public K floorKey(K maxKey) {
        return this.findKey(maxKey, -1, 0);
    }

    @Override
    public Map.Entry<K, V> floorEntry(K maxKey) {
        return this.findEntry(maxKey, -1, 0);
    }

    @Override
    public K higherKey(K minKey) {
        return this.findKey(minKey, 0, 1);
    }

    @Override
    public Map.Entry<K, V> higherEntry(K minKey) {
        return this.findEntry(minKey, 0, 1);
    }

    @Override
    public K ceilingKey(K minKey) {
        return this.findKey(minKey, 0, 0);
    }

    @Override
    public Map.Entry<K, V> ceilingEntry(K minKey) {
        return this.findEntry(minKey, 0, 0);
    }

    @Override
    public Map.Entry<K, V> pollFirstEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map.Entry<K, V> pollLastEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return new ImmutableNavigableSet<>(this.bounds, this.keys, this.minIndex, this.maxIndex, this.comparator);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ImmutableNavigableSet<Map.Entry<K, V>> entrySet() {
        final int size = this.size();
        final AbstractMap.SimpleImmutableEntry<K, V>[] entries
          = (AbstractMap.SimpleImmutableEntry<K, V>[])new AbstractMap.SimpleImmutableEntry<?, ?>[size];
        for (int i = this.minIndex; i < this.maxIndex; i++)
            entries[i - this.minIndex] = this.createEntry(i);
        return new ImmutableNavigableSet<>(new Bounds<>(), entries, Comparator.comparing(Map.Entry::getKey, this.actualComparator));
    }

    @Override
    protected ImmutableNavigableMap<K, V> createSubMap(boolean reverse, Bounds<K> newBounds) {

        // Get upper and lower bounds; note: "newBounds" are already reversed
        final K minBound = reverse ? newBounds.getUpperBound() : newBounds.getLowerBound();
        final K maxBound = reverse ? newBounds.getLowerBound() : newBounds.getUpperBound();
        final BoundType minBoundType = reverse ? newBounds.getUpperBoundType() : newBounds.getLowerBoundType();
        final BoundType maxBoundType = reverse ? newBounds.getLowerBoundType() : newBounds.getUpperBoundType();

        // Calculate the index range in our current array corresponding to the new bounds
        final int newMinIndex;
        switch (minBoundType) {
        case INCLUSIVE:
            newMinIndex = this.findNearby(minBound, 0, 0);
            break;
        case EXCLUSIVE:
            newMinIndex = this.findNearby(minBound, 0, 1);
            break;
        case NONE:
            newMinIndex = this.minIndex;
            break;
        default:
            throw new RuntimeException("internal error");
        }
        final int newMaxIndex;
        switch (maxBoundType) {
        case INCLUSIVE:
            newMaxIndex = this.findNearby(maxBound, 0, 1);
            break;
        case EXCLUSIVE:
            newMaxIndex = this.findNearby(maxBound, 0, 0);
            break;
        case NONE:
            newMaxIndex = this.maxIndex;
            break;
        default:
            throw new RuntimeException("internal error");
        }

        // Create new instance
        if (reverse) {
            return new ImmutableNavigableMap<K, V>(newBounds,
              ImmutableNavigableSet.reverseArray(Arrays.copyOfRange(this.keys, newMinIndex, newMaxIndex)),
              ImmutableNavigableSet.reverseArray(Arrays.copyOfRange(this.vals, newMinIndex, newMaxIndex)),
              ImmutableNavigableSet.reversedComparator(this.comparator));
        } else
            return new ImmutableNavigableMap<K, V>(newBounds, this.keys, this.vals, newMinIndex, newMaxIndex, this.comparator);
    }

    private K findKey(Object key, int notFoundOffset, int foundOffset) {
        final int index = this.findNearby(key, notFoundOffset, foundOffset);
        if (index < this.minIndex || index >= this.maxIndex)
            return null;
        return this.keys[index];
    }

    private Map.Entry<K, V> findEntry(Object key, int notFoundOffset, int foundOffset) {
        final int index = this.findNearby(key, notFoundOffset, foundOffset);
        if (index < this.minIndex || index >= this.maxIndex)
            return null;
        return this.createEntry(index);
    }

    private int findNearby(Object key, int notFoundOffset, int foundOffset) {
        final int index = this.find(key);
        return index < 0 ? ~index + notFoundOffset : index + foundOffset;
    }

    @SuppressWarnings("unchecked")
    private int find(Object key) {
        return Arrays.binarySearch(this.keys, this.minIndex, this.maxIndex, (K)key, this.actualComparator);
    }

    private int checkIndex(final int index) {
        if (index < this.minIndex || index >= this.maxIndex)
            throw new NoSuchElementException();
        return index;
    }

    private AbstractMap.SimpleImmutableEntry<K, V> createEntry(int index) {
        assert index >= this.minIndex && index < this.maxIndex;
        return new AbstractMap.SimpleImmutableEntry<>(this.keys[index], this.vals[index]);
    }
}

