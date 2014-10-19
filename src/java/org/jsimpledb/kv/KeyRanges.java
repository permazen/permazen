
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
     * Creates an instance containing all of the given individual ranges. The given ranges may overlap
     * and be listed in any order.
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

// Instance methods

    /**
     * Get the "minimal" set of {@link KeyRange}s whose union is equivalent to this instance,
     * where "minimal" means no two ranges are adjacent or overlapping.
     *
     * <p>
     * The {@link KeyRange}s in the returned list will be sorted by their minimum keys.
     * </p>
     *
     * @return minimal, unmodifiable list of {@link KeyRange}s sorted by range minimum
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
        return this.getKeyRange(key) != null;
    }

    /**
     * Find the maximal contiguous {@link KeyRange} containing the given key, if any.
     *
     * @param key key to find
     * @return maximal contiguous {@link KeyRange} in this instance containing {@code key}, or null if there is none
     * @throws IllegalArgumentException if {@code key} is null
     */
    public KeyRange getKeyRange(byte[] key) {
        if (key == null)
            throw new IllegalArgumentException("null key");
        int i = Collections.binarySearch(this.ranges, new KeyRange(key, null), KeyRange.SORT_BY_MIN);
        if (i >= 0)
            return this.ranges.get(i);
        i = ~i;
        if (i == 0)
            return null;
        final KeyRange range = this.ranges.get(i - 1);
        return range.contains(key) ? range : null;
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
                prev = new KeyRange(prev.getMin(), range.getMax());
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

