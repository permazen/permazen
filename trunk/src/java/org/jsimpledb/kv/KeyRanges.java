
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jsimpledb.util.ByteUtil;

/**
 * A collection of zero or more {@link KeyRange}s.
 * Instances are immutable.
 *
 * @see KeyRange
 */
public class KeyRanges {

    /**
     * The empty instance containing zero ranges.
     */
    public static final KeyRanges EMPTY = new KeyRanges(Collections.<KeyRange>emptyList());

    /**
     * The "full" instance containing a single {@link KeyRange} that contains all keys.
     */
    public static final KeyRanges FULL = new KeyRanges(Arrays.asList(KeyRange.FULL));

    private final List<KeyRange> ranges;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Creates an instance that contains all keys contained by any of the {@link KeyRange}s in {@code ranges}.
     * The given {@code ranges} may be adjacent, overlap, and/or be listed in any order.
     * </p>
     *
     * @param ranges individual key ranges
     * @throws IllegalArgumentException if {@code ranges} or any {@link KeyRange} therein is null
     */
    public KeyRanges(List<KeyRange> ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        this.ranges = KeyRanges.minimize(ranges);
    }

    /**
     * Constructor for an instance containing a single range.
     *
     * @param range single range
     * @throws IllegalArgumentException if {@code range} is null
     */
    public KeyRanges(KeyRange range) {
        this(Collections.singletonList(range));
    }

    /**
     * Constructor for an instance containing a single range.
     *
     * @param min minimum key (inclusive), or null for no minimum
     * @param max maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code min > max}
     */
    public KeyRanges(byte[] min, byte[] max) {
        this(new KeyRange(min, max));
    }

    /**
     * Construct an instance containing a single range corresponding to all keys with the given prefix.
     *
     * @param prefix prefix of all keys in the range
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public static KeyRanges forPrefix(byte[] prefix) {
        return new KeyRanges(KeyRange.forPrefix(prefix));
    }

// Instance methods

    /**
     * Get the minimal set of {@link KeyRange}s whose union is equivalent to this instance.
     * Here "minimal" means no two ranges are adjacent or overlap.
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

    /**
     * Create the inverse of this instance. The inverse contains all keys not contained by this instance.
     *
     * @return the inverse of this instance
     */
    public KeyRanges inverse() {
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
        return new KeyRanges(list);
    }

    /**
     * Determine whether this instance contains the given key.
     *
     * @param key key to test
     * @return true if {@code key} is contained by this instance, otherwise false
     * @throws IllegalArgumentException if {@code key} is null
     */
    public boolean contains(byte[] key) {
        final KeyRange[] pair = this.findKey(key);
        return pair[0] == pair[1] && pair[0] != null;
    }

    /**
     * Determine whether this instance contains the given {@link KeyRanges}, i.e., all keys contained by
     * the given {@link KeyRanges} are also contained by this instance.
     *
     * @param ranges other instance to test
     * @return true if this instance contains {@code ranges}, otherwise false
     * @throws IllegalArgumentException if {@code ranges} is null
     */
    public boolean contains(KeyRanges ranges) {
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
        if (key == null)
            throw new IllegalArgumentException("null key");
        final int i = ~Collections.binarySearch(this.ranges, new KeyRange(key, key), KeyRange.SORT_BY_MIN);
        assert i >= 0;                                                  // this.ranges should never contain new KeyRange(key, key)
        KeyRange left = null;
        if (i > 0) {
            if ((left = this.ranges.get(i - 1)).contains(key))
                return new KeyRange[] { left, left };
        }
        KeyRange right = null;
        if (i < this.ranges.size()) {
            if ((right = this.ranges.get(i)).contains(key))
                return new KeyRange[] { right, right };
        }
        return new KeyRange[] { left, right };
    }

    /**
     * Create an instance that represents the union of this and the provided instance.
     *
     * @param ranges other instance
     * @return the union of this instance and {@code ranges}
     */
    public KeyRanges union(KeyRanges ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        final ArrayList<KeyRange> list = new ArrayList<>(this.ranges.size() + ranges.ranges.size());
        list.addAll(this.ranges);
        list.addAll(ranges.ranges);
        return new KeyRanges(list);
    }

    /**
     * Create an instance that represents the intersection of this and the provided instance.
     *
     * @param ranges other instance
     * @return the intersection of this instance and {@code ranges}
     */
    public KeyRanges intersection(KeyRanges ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        return this.inverse().union(ranges.inverse()).inverse();
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final KeyRanges that = (KeyRanges)obj;
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

