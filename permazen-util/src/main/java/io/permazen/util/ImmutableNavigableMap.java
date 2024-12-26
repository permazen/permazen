
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

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
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * An immutable {@link NavigableMap} implementation optimized for read efficiency.
 *
 * <p>
 * Because the keys and values are stored in arrays, it's also possible to get the key and/or value by index;
 * see {@link #getKey getKey()}, {@link #getValue getValue()}, and {@link #getEntry getEntry()}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("serial")
public class ImmutableNavigableMap<K, V> extends AbstractNavigableMap<K, V> implements ConcurrentNavigableMap<K, V> {

    private final K[] keys;
    private final V[] vals;
    private final int minIndex;
    private final int maxIndex;
    private final Comparator<? super K> comparator;
    private final Comparator<? super K> actualComparator;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Sorts via natural ordering.
     *
     * @param source data source
     * @throws IllegalArgumentException if {@code source} is null
     */
    public ImmutableNavigableMap(NavigableMap<K, V> source) {
        this(source, checkNull(source).comparator());
    }

    /**
     * Constructor.
     *
     * @param source data source
     * @param comparator key comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code source} is null
     */
    @SuppressWarnings("unchecked")
    public ImmutableNavigableMap(NavigableMap<K, V> source, Comparator<? super K> comparator) {
        this((K[])checkNull(source).keySet().toArray(), (V[])source.values().toArray(), comparator);
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Sorts via natural ordering.
     *
     * <p>
     * Equivalent to: {@code ImmutableNavigableMap(keys, vals, null)}.
     *
     * @param keys sorted key array; <i>this array is not copied and must be already sorted</i>
     * @param vals value array corresponding to {@code keys}; <i>this array is not copied</i>
     * @throws IllegalArgumentException if {@code keys} or {@code vals} is null
     * @throws IllegalArgumentException if {@code keys} and {@code vals} have different lengths
     */
    public ImmutableNavigableMap(K[] keys, V[] vals) {
        this(keys, vals, null);
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to: {@code ImmutableNavigableMap(keys, vals, 0, Math.min(keys.length, vals.length), comparator)}.
     *
     * @param keys sorted key array; <i>this array is not copied and must be already sorted</i>
     * @param vals value array corresponding to {@code keys}; <i>this array is not copied</i>
     * @param comparator key comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code keys} or {@code vals} is null
     * @throws IllegalArgumentException if {@code keys} and {@code vals} have different lengths
     */
    public ImmutableNavigableMap(K[] keys, V[] vals, Comparator<? super K> comparator) {
        this(new Bounds<>(), checkNull(keys), checkNull(vals), 0, Math.min(keys.length, vals.length), comparator);
    }

    /**
     * Primary constructor.
     *
     * @param keys sorted key array; <i>this array is not copied and must be already sorted</i>
     * @param vals value array corresponding to {@code keys}; <i>this array is not copied</i>
     * @param minIndex minimum index into arrays (inclusive)
     * @param maxIndex maximum index into arrays (exclusive)
     * @param comparator key comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code keys} or {@code vals} is null
     * @throws IllegalArgumentException if {@code keys} and {@code vals} has length less than {@code maxIndex}
     * @throws IllegalArgumentException if {@code minIndex > maxIndex}
     */
    public ImmutableNavigableMap(K[] keys, V[] vals, int minIndex, int maxIndex, Comparator<? super K> comparator) {
        this(new Bounds<>(), keys, vals, minIndex, maxIndex, comparator);
    }

    @SuppressWarnings("unchecked")
    private ImmutableNavigableMap(Bounds<K> bounds, K[] keys, V[] vals,
      int minIndex, int maxIndex, Comparator<? super K> comparator) {
        super(bounds);
        Preconditions.checkArgument(keys != null);
        Preconditions.checkArgument(vals != null);
        Preconditions.checkArgument(minIndex >= 0 && maxIndex >= minIndex);
        Preconditions.checkArgument(keys.length >= maxIndex);
        Preconditions.checkArgument(vals.length >= maxIndex);
        this.keys = keys;
        this.vals = vals;
        this.minIndex = minIndex;
        this.maxIndex = maxIndex;
        this.comparator = comparator;
        this.actualComparator = NavigableSets.comparatorOrNatural(this.comparator);
        for (int i = minIndex + 1; i < maxIndex; i++)
            assert this.actualComparator.compare(this.keys[i - 1], this.keys[i]) < 0;
    }

// Extra Methods

    /**
     * Get the key at the specified index.
     *
     * @param index index into the ordered key array
     * @return the key at the specified index
     * @throws IndexOutOfBoundsException if {@code index} is negative or greater than or equal to {@link #size}
     */
    public K getKey(int index) {
        Objects.checkIndex(index, this.size());
        return this.keys[this.minIndex + index];
    }

    /**
     * Get the value at the specified index.
     *
     * @param index index into the ordered value array
     * @return the value at the specified index
     * @throws IndexOutOfBoundsException if {@code index} is negative or greater than or equal to {@link #size}
     */
    public V getValue(int index) {
        Objects.checkIndex(index, this.size());
        return this.vals[this.minIndex + index];
    }

    /**
     * Get the entry at the specified index.
     *
     * @param index index into the ordered entry array
     * @return the entry at the specified index
     * @throws IndexOutOfBoundsException if {@code index} is negative or greater than or equal to {@link #size}
     */
    public Map.Entry<K, V> getEntry(int index) {
        Objects.checkIndex(index, this.size());
        return this.createEntry(this.minIndex + index);
    }

    /**
     * Search for the given element in the underlying array.
     *
     * <p>
     * This method works like {@link Arrays#binarySearch(Object[], Object) Arrays.binarySearch()}, returning
     * either the index of {@code key} in the underlying array given to the constructor if found, or else
     * the one's complement of {@code key}'s insertion point.
     *
     * <p>
     * The array searched is the array given to the constructor, or if {@link #ImmutableNavigableMap(NavigableMap)}
     * was used, an array containing all of the keys in this map.
     *
     * @param key key to search for
     * @return index of {@code key}, or {@code -(insertion point) - 1} if not found
     */
    public int binarySearch(K key) {
        return this.find(key);
    }

// NavigableMap

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
        return new ImmutableNavigableSet<>(entries, Comparator.comparing(Map.Entry::getKey, this.actualComparator));
    }

// ConcurrentNavigableMap

    @Override
    public V putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

// Narrowing Overrides

    @Override
    public ImmutableNavigableMap<K, V> descendingMap() {
        return (ImmutableNavigableMap<K, V>)super.descendingMap();
    }

    @Override
    public ImmutableNavigableMap<K, V> subMap(K minKey, K maxKey) {
        return (ImmutableNavigableMap<K, V>)super.subMap(minKey, maxKey);
    }

    @Override
    public ImmutableNavigableMap<K, V> headMap(K maxKey) {
        return (ImmutableNavigableMap<K, V>)super.headMap(maxKey);
    }

    @Override
    public ImmutableNavigableMap<K, V> tailMap(K minKey) {
        return (ImmutableNavigableMap<K, V>)super.tailMap(minKey);
    }

    @Override
    public ImmutableNavigableMap<K, V> headMap(K newMaxKey, boolean inclusive) {
        return (ImmutableNavigableMap<K, V>)super.headMap(newMaxKey, inclusive);
    }

    @Override
    public ImmutableNavigableMap<K, V> tailMap(K newMinKey, boolean inclusive) {
        return (ImmutableNavigableMap<K, V>)super.tailMap(newMinKey, inclusive);
    }

    @Override
    public ImmutableNavigableMap<K, V> subMap(K newMinKey, boolean minInclusive, K newMaxKey, boolean maxInclusive) {
        return (ImmutableNavigableMap<K, V>)super.subMap(newMinKey, minInclusive, newMaxKey, maxInclusive);
    }

// AbstractNavigableMap

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
            final int newSize = newMaxIndex - newMinIndex;
            return new ImmutableNavigableMap<K, V>(newBounds,
              ImmutableNavigableSet.reverseArray(Arrays.copyOfRange(this.keys, newMinIndex, newMaxIndex)),
              ImmutableNavigableSet.reverseArray(Arrays.copyOfRange(this.vals, newMinIndex, newMaxIndex)),
              0, newSize, ImmutableNavigableSet.reversedComparator(this.comparator));
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

    private static <T> T checkNull(T obj) {
        if (obj == null)
            throw new IllegalArgumentException();
        return obj;
    }

    private AbstractMap.SimpleImmutableEntry<K, V> createEntry(int index) {
        assert index >= this.minIndex && index < this.maxIndex;
        return new AbstractMap.SimpleImmutableEntry<>(this.keys[index], this.vals[index]);
    }
}
