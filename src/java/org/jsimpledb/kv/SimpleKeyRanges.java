
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jsimpledb.util.ByteUtil;

/**
 * Straightforward implementation of the {@link KeyRanges} interface based on a finite, fixed set of {@link KeyRange} instances.
 *
 * <p>
 * Instances are immutable.
 * </p>
 *
 * @see KeyRange
 */
public class SimpleKeyRanges implements KeyRanges {

    private final List<KeyRange> ranges;

    private volatile KeyRange lastContainingKeyRange;           // used for optimization

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Creates an instance that contains all keys contained by any of the {@link KeyRange}s in {@code ranges}.
     * The given {@code ranges} may be adjacent, overlap, and/or be listed in any order; this constructor
     * will normalize them.
     * </p>
     *
     * @param ranges individual key ranges
     * @throws IllegalArgumentException if {@code ranges} or any {@link KeyRange} therein is null
     */
    public SimpleKeyRanges(List<KeyRange> ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        this.ranges = SimpleKeyRanges.minimize(ranges);
    }

    /**
     * Constructor for an instance containing a single range.
     *
     * @param range single range
     * @throws IllegalArgumentException if {@code range} is null
     */
    public SimpleKeyRanges(KeyRange range) {
        this(Collections.singletonList(range));
    }

    /**
     * Constructor for an instance containing a single range.
     *
     * @param min minimum key (inclusive), or null for no minimum
     * @param max maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code min > max}
     */
    public SimpleKeyRanges(byte[] min, byte[] max) {
        this(new KeyRange(min, max));
    }

    /**
     * Construct an instance containing a single range corresponding to all keys with the given prefix.
     *
     * @param prefix prefix of all keys in the range
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public static SimpleKeyRanges forPrefix(byte[] prefix) {
        return new SimpleKeyRanges(KeyRange.forPrefix(prefix));
    }

// Instance methods

    /**
     * Get {@link KeyRange}s underlying with this instance.
     *
     * <p>
     * The returned {@link KeyRange}s will be listed in order.
     * </p>
     *
     * @return minimal, unmodifiable list of {@link KeyRange}s sorted by key range
     */
    public List<KeyRange> getKeyRanges() {
        return Collections.unmodifiableList(this.ranges);
    }

    /**
     * Determine whether this instance is empty, i.e., contains no keys.
     */
    public boolean isEmpty() {
        return this.ranges.isEmpty();
    }

    /**
     * Determine whether this instance is "full", i.e., contains all keys.
     */
    public boolean isFull() {
        return this.ranges.size() == 1 && this.ranges.get(0).isFull();
    }

    /**
     * Get the minimum key contained by this instance (inclusive).
     *
     * @return minimum key contained by this instance (inclusive),
     *  or null if there is no lower bound, or this instance {@link #isEmpty}
     */
    public byte[] getMin() {
        return !this.ranges.isEmpty() ? this.ranges.get(0).getMin() : null;
    }

    /**
     * Get the maximum key contained by this instance (exclusive).
     *
     * @return maximum key contained by this instance (exclusive),
     *  or null if there is no upper bound, or this instance {@link #isEmpty}
     */
    public byte[] getMax() {
        final int numRanges = this.ranges.size();
        return numRanges > 0 ? this.ranges.get(numRanges - 1).getMax() : null;
    }

    @Override
    public SimpleKeyRanges inverse() {
        final ArrayList<KeyRange> list = new ArrayList<>(this.ranges.size() + 1);
        if (this.ranges.isEmpty())
            return KeyRanges.FULL;
        final KeyRange first = this.ranges.get(0);
        int i = 0;
        byte[] lastMax;
        if (first.getMin() == null) {
            if ((lastMax = first.getMax()) == null) {
                assert this.ranges.size() == 1;
                return KeyRanges.EMPTY;
            }
            i++;
        } else
            lastMax = null;
        while (true) {
            if (i == this.ranges.size()) {
                list.add(new KeyRange(lastMax, null));
                break;
            }
            final KeyRange next = this.ranges.get(i++);
            assert next.getMin() != null;
            list.add(new KeyRange(lastMax, next.getMin()));
            if ((lastMax = next.getMax()) == null) {
                assert i == this.ranges.size();
                break;
            }
        }
        return new SimpleKeyRanges(list);
    }

    /**
     * Determine whether this instance contains the given {@link SimpleKeyRanges}, i.e., all keys contained by
     * the given {@link SimpleKeyRanges} are also contained by this instance.
     *
     * @param ranges other instance to test
     * @return true if this instance contains {@code ranges}, otherwise false
     * @throws IllegalArgumentException if {@code ranges} is null
     */
    public boolean contains(SimpleKeyRanges ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        return ranges.equals(this.intersection(ranges));
    }

    /**
     * Find the contiguous {@link KeyRange}(s) within this instance containing, or adjacent to, the given key.
     *
     * <p>
     * This method returns an array of length two: if this instance contains {@code key} then both elements are the
     * same Java object, namely, the {@link KeyRange} that contains {@code key}; otherwise, the first element is
     * the nearest {@link KeyRange} to the left of {@code key}, or null if none exists, and the second element is
     * the {@link KeyRange} to the right of {@code key}, or null if none exists. Note if this instance is empty
     * then <code>{ null, null }</code> is returned.
     * </p>
     *
     * @param key key to find
     * @return array with the containing {@link KeyRange} or nearest neighbor to the left (or null) and
     *  the containing {@link KeyRange} or nearest neighbor to the right (or null)
     * @throws IllegalArgumentException if {@code key} is null
     */
    public KeyRange[] findKey(byte[] key) {

        // Sanity check
        if (key == null)
            throw new IllegalArgumentException("null key");

        // Optimization: assume previous success is likely to repeat
        final KeyRange temp = this.lastContainingKeyRange;
        if (temp != null) {
            if (temp.contains(key))
                return new KeyRange[] { temp, temp };
            this.lastContainingKeyRange = null;
        }

        // Search for matching range
        final int i = ~Collections.binarySearch(this.ranges, new KeyRange(key, key), KeyRange.SORT_BY_MIN);
        assert i >= 0;                                                  // this.ranges should never contain new KeyRange(key, key)
        KeyRange left = null;
        if (i > 0) {
            if ((left = this.ranges.get(i - 1)).contains(key)) {
                this.lastContainingKeyRange = left;
                return new KeyRange[] { left, left };
            }
        }
        KeyRange right = null;
        if (i < this.ranges.size()) {
            if ((right = this.ranges.get(i)).contains(key)) {
                this.lastContainingKeyRange = right;
                return new KeyRange[] { right, right };
            }
        }

        // Not contained
        return new KeyRange[] { left, right };
    }

    /**
     * Create an instance that represents the union of this and the provided instance(s).
     *
     * @param others other instances
     * @return the union of this instance and {@code others}
     * @throws IllegalArgumentException if {@code others} or any element in {@code others} is null
     */
    public SimpleKeyRanges union(SimpleKeyRanges... others) {
        if (others == null)
            throw new IllegalArgumentException("null others");
        if (others.length == 0)
            return this;
        final ArrayList<KeyRange> list = new ArrayList<>(this.ranges.size() + others.length);
        list.addAll(this.ranges);
        for (SimpleKeyRanges other : others) {
            if (other == null)
                throw new IllegalArgumentException("null other");
            list.addAll(other.ranges);
        }
        return new SimpleKeyRanges(list);
    }

    /**
     * Create an instance that represents the intersection of this and the provided instance(s).
     *
     * @param others other instances
     * @return the intersection of this instance and {@code others}
     * @throws IllegalArgumentException if {@code others} or any element in {@code others} is null
     */
    public SimpleKeyRanges intersection(SimpleKeyRanges... others) {
        if (others == null)
            throw new IllegalArgumentException("null others");
        if (others.length == 0)
            return this;
        final ArrayList<KeyRange> list = new ArrayList<>(this.ranges.size() + others.length);
        list.addAll(this.inverse().ranges);
        for (SimpleKeyRanges other : others) {
            if (other == null)
                throw new IllegalArgumentException("null other");
            list.addAll(other.inverse().ranges);
        }
        return new SimpleKeyRanges(list).inverse();
    }

// KeyRanges

    @Override
    public boolean contains(byte[] key) {
        final KeyRange[] pair = this.findKey(key);
        return pair[0] == pair[1] && pair[0] != null;
    }

    @Override
    public KeyRange nextHigherRange(byte[] key) {
        return this.findKey(key)[1];
    }

    @Override
    public KeyRange nextLowerRange(byte[] key) {
        return this.findKey(key)[0];
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SimpleKeyRanges that = (SimpleKeyRanges)obj;
        return this.ranges.equals(that.ranges);
    }

    @Override
    public int hashCode() {
        return this.ranges.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append(this.getClass().getSimpleName()).append("[");
        boolean first = true;
        for (KeyRange range : this.ranges) {
            if (first)
                first = false;
            else
                buf.append(",");
            buf.append(range);
        }
        buf.append("]");
        return buf.toString();
    }

// Internal methods

    // Return a "minimal" list with these properties:
    //  - Sorted according to KeyRange.SORT_BY_MIN
    //  - No overlapping ranges
    //  - Adjacent ranges consolidated into a single range
    private static ArrayList<KeyRange> minimize(List<KeyRange> ranges) {
        final ArrayList<KeyRange> sortedRanges = new ArrayList<>(ranges);
        Collections.sort(sortedRanges, KeyRange.SORT_BY_MIN);
        final ArrayList<KeyRange> list = new ArrayList<>(ranges.size());
        KeyRange prev = null;
        for (KeyRange range : sortedRanges) {
            if (range == null)
                throw new IllegalArgumentException("null range");
            if (range.getMin() != null && range.getMax() != null && ByteUtil.compare(range.getMin(), range.getMax()) == 0)
                continue;
            if (prev == null) {                         // range is the first in the list
                prev = range;
                continue;
            }
            final int diff1 = KeyRange.compare(range.getMin(), KeyRange.MIN, prev.getMin(), KeyRange.MIN);
            assert diff1 >= 0;
            if (diff1 == 0) {                           // range contains prev -> discard prev
                assert range.contains(prev);
                prev = range;
                continue;
            }
            final int diff2 = KeyRange.compare(range.getMin(), KeyRange.MIN, prev.getMax(), KeyRange.MAX);
            if (diff2 <= 0) {                           // prev and range overlap -> take their union
                final byte[] max = KeyRange.compare(range.getMax(), KeyRange.MAX, prev.getMax(), KeyRange.MAX) > 0 ?
                  range.getMax() : prev.getMax();
                prev = new KeyRange(prev.getMin(), max);
                continue;
            }
            list.add(prev);                             // prev and range don't overlap -> accept prev
            prev = range;
            continue;
        }
        if (prev != null)
            list.add(prev);
        list.trimToSize();
        return list;
    }
}

