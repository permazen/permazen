
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Preconditions;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;

/**
 * Support superclass for {@link NavigableMap} implementations.
 *
 * <p>
 * For a read-only implementation, subclasses should implement {@link #comparator comparator()}, {@link #get get()},
 * {@link #entrySet entrySet()}, {@link #navigableKeySet navigableKeySet()}, and {@link #createSubMap createSubMap()}
 * to handle reversed and restricted range sub-maps.
 * </p>
 *
 * <p>
 * For a mutable implementation, subclasses should also implement {@link #put put()}, {@link #remove remove()},
 * {@link #clear clear()}, and make the {@link #keySet keySet()} and {@link #entrySet entrySet()} iterators mutable.
 * </p>
 *
 * <p>
 * All overridden methods must be aware of the {@linkplain #bounds range restriction bounds}, if any.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class AbstractNavigableMap<K, V> extends AbstractMap<K, V> implements NavigableMap<K, V> {

    /**
     * Key range bounds associated with this instance.
     */
    protected final Bounds<K> bounds;

    /**
     * Convenience constructor for the case where there are no lower or upper key bounds.
     */
    protected AbstractNavigableMap() {
        this(new Bounds<K>());
    }

    /**
     * Primary constructor.
     *
     * @param bounds key range restriction
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected AbstractNavigableMap(Bounds<K> bounds) {
        Preconditions.checkArgument(bounds != null, "null bounds");
        this.bounds = bounds;
    }

    @Override
    public boolean isEmpty() {
        return this.navigableKeySet().isEmpty();
    }

    @Override
    public boolean containsKey(Object obj) {
        return this.navigableKeySet().contains(obj);
    }

    @Override
    public K firstKey() {
        return this.navigableKeySet().iterator().next();
    }

    @Override
    public K lastKey() {
        return this.descendingMap().firstKey();
    }

    @Override
    public NavigableSet<K> keySet() {
        return this.navigableKeySet();
    }

    @Override
    public Map.Entry<K, V> lowerEntry(K maxKey) {
        return this.searchBelow(maxKey, false);
    }

    @Override
    public Map.Entry<K, V> floorEntry(K maxKey) {
        return this.searchBelow(maxKey, true);
    }

    @Override
    public Map.Entry<K, V> ceilingEntry(K minKey) {
        return this.searchAbove(minKey, true);
    }

    @Override
    public Map.Entry<K, V> higherEntry(K minKey) {
        return this.searchAbove(minKey, false);
    }

    @Override
    public K lowerKey(K maxKey) {
        final Map.Entry<K, V> entry = this.lowerEntry(maxKey);
        return entry != null ? entry.getKey() : null;
    }

    @Override
    public K floorKey(K maxKey) {
        final Map.Entry<K, V> entry = this.floorEntry(maxKey);
        return entry != null ? entry.getKey() : null;
    }

    @Override
    public K ceilingKey(K minKey) {
        final Map.Entry<K, V> entry = this.ceilingEntry(minKey);
        return entry != null ? entry.getKey() : null;
    }

    @Override
    public K higherKey(K minKey) {
        final Map.Entry<K, V> entry = this.higherEntry(minKey);
        return entry != null ? entry.getKey() : null;
    }

    @Override
    public Map.Entry<K, V> firstEntry() {
        try {
            return this.entrySet().iterator().next();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public Map.Entry<K, V> lastEntry() {
        return this.descendingMap().firstEntry();
    }

    @Override
    public Map.Entry<K, V> pollFirstEntry() {
        final Iterator<Map.Entry<K, V>> i = this.entrySet().iterator();
        if (!i.hasNext())
            return null;
        final Map.Entry<K, V> entry = i.next();
        i.remove();
        return entry;
    }

    @Override
    public Map.Entry<K, V> pollLastEntry() {
        return this.descendingMap().pollFirstEntry();
    }

    @Override
    public NavigableMap<K, V> descendingMap() {
        return this.createSubMap(true, this.bounds.reverse());
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return this.navigableKeySet().descendingSet();
    }

    @Override
    public NavigableMap<K, V> subMap(K minKey, K maxKey) {
        return this.subMap(minKey, true, maxKey, false);
    }

    @Override
    public NavigableMap<K, V> headMap(K maxKey) {
        return this.headMap(maxKey, false);
    }

    @Override
    public NavigableMap<K, V> tailMap(K minKey) {
        return this.tailMap(minKey, true);
    }

    @Override
    public NavigableMap<K, V> headMap(K newMaxKey, boolean inclusive) {
        final Bounds<K> newBounds = this.bounds.withUpperBound(newMaxKey, BoundType.of(inclusive));
        if (!this.bounds.isWithinBounds(this.comparator(), newBounds))
            throw new IllegalArgumentException("upper bound " + newMaxKey + " is out of bounds: " + this.bounds);
        return this.createSubMap(false, newBounds);
    }

    @Override
    public NavigableMap<K, V> tailMap(K newMinKey, boolean inclusive) {
        final Bounds<K> newBounds = this.bounds.withLowerBound(newMinKey, BoundType.of(inclusive));
        if (!this.bounds.isWithinBounds(this.comparator(), newBounds))
            throw new IllegalArgumentException("lower bound " + newMinKey + " is out of bounds: " + this.bounds);
        return this.createSubMap(false, newBounds);
    }

    @Override
    public NavigableMap<K, V> subMap(K newMinKey, boolean minInclusive, K newMaxKey, boolean maxInclusive) {
        final Bounds<K> newBounds = new Bounds<>(newMinKey, BoundType.of(minInclusive), newMaxKey, BoundType.of(maxInclusive));
        if (!this.bounds.isWithinBounds(this.comparator(), newBounds))
            throw new IllegalArgumentException("new bound(s) " + newBounds + " are out of bounds: " + this.bounds);
        return this.createSubMap(false, newBounds);
    }

    /**
     * Search for a lower element. Used to implement {@link #floorEntry floorEntry()} and {@link #lowerEntry lowerEntry()}.
     *
     * <p>
     * The implementation in {@link AbstractNavigableMap} checks the bounds then returns the first entry from a head map.
     * </p>
     *
     * @param maxKey upper limit for search
     * @param inclusive true if {@code maxKey} itself is a candidate
     * @return highest element below {@code maxKey}, or null if not found
     */
    protected Map.Entry<K, V> searchBelow(K maxKey, boolean inclusive) {
        if (!this.isWithinLowerBound(maxKey))
            return null;
        final NavigableMap<K, V> subMap = this.isWithinUpperBound(maxKey) ? this.headMap(maxKey, inclusive) : this;
        try {
            return subMap.lastEntry();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Search for a higher element. Used to implement {@link #ceilingEntry ceilingEntry()} and {@link #higherEntry higherEntry()}.
     *
     * <p>
     * The implementation in {@link AbstractNavigableMap} checks the bounds then returns the first entry from a tail map.
     * </p>
     *
     * @param minKey lower limit for search
     * @param inclusive true if {@code minKey} itself is a candidate
     * @return lowest element above {@code minKey}, or null if not found
     */
    protected Map.Entry<K, V> searchAbove(K minKey, boolean inclusive) {
        if (!this.isWithinUpperBound(minKey))
            return null;
        final NavigableMap<K, V> subMap = this.isWithinLowerBound(minKey) ? this.tailMap(minKey, inclusive) : this;
        try {
            return subMap.firstEntry();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Get a non-null {@link Comparator} that sorts consistently with, and optionally reversed from, this instance.
     *
     * @param reversed whether to return a reversed {@link Comparator}
     * @return a non-null {@link Comparator}
     */
    protected Comparator<? super K> getComparator(boolean reversed) {
        return NavigableSets.getComparator(this.comparator(), reversed);
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds.
     * The {@code newBounds} are consistent with the new ordering (i.e., reversed relative to this instance's ordering if
     * {@code reverse} is true) and have already been range-checked against {@linkplain #bounds this instance's current bounds}.
     *
     * @param reverse whether the new map's ordering should be reversed relative to this instance's ordering
     * @param newBounds new bounds
     * @return restricted and/or reversed view of this instance
     * @throws IllegalArgumentException if {@code newBounds} is null
     * @throws IllegalArgumentException if a bound in {@code newBounds} is null and this set does not permit null elements
     */
    protected abstract NavigableMap<K, V> createSubMap(boolean reverse, Bounds<K> newBounds);

    /**
     * Determine if the given element is within this instance's lower bound (if any).
     *
     * <p>
     * The implementation in {@link AbstractNavigableMap} returns {@code this.bounds.isWithinLowerBound(this.comparator(), elem)}.
     * </p>
     *
     * @param key map key
     * @return true if {@code elem} is within this instance's lower bound, or this instance has no lower bound
     */
    protected boolean isWithinLowerBound(K key) {
        return this.bounds.isWithinLowerBound(this.comparator(), key);
    }

    /**
     * Determine if the given element is within this instance's upper bound (if any).
     *
     * <p>
     * The implementation in {@link AbstractNavigableMap} returns {@code this.bounds.isWithinUpperBound(this.comparator(), elem)}.
     * </p>
     *
     * @param key map key
     * @return true if {@code elem} is within this instance's upper bound, or this instance has no upper bound
     */
    protected boolean isWithinUpperBound(K key) {
        return this.bounds.isWithinUpperBound(this.comparator(), key);
    }
}

