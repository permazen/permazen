
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.jsimpledb.kv.util.KeyListEncoder;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.SizeEstimating;
import org.jsimpledb.util.SizeEstimator;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * A fixed set of {@link KeyRange} instances that can be treated as a unified whole, in particular as a {@link KeyFilter}.
 *
 * <p>
 * Instances are immutable.
 * </p>
 *
 * @see KeyRange
 */
public class KeyRanges implements Iterable<KeyRange>, KeyFilter, SizeEstimating {

    /**
     * The empty instance containing zero ranges.
     */
    public static final KeyRanges EMPTY = new KeyRanges(Collections.<KeyRange>emptyList());

    /**
     * The "full" instance containing a single {@link KeyRange} that contains all keys.
     */
    public static final KeyRanges FULL = new KeyRanges(Arrays.asList(KeyRange.FULL));

    private final ArrayList<KeyRange> ranges;

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
    public KeyRanges(Iterable<? extends KeyRange> ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        this.ranges = KeyRanges.minimize(Lists.<KeyRange>newArrayList(ranges));
    }

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
    public KeyRanges(KeyRange... ranges) {
        this(ranges != null ? Arrays.asList(ranges) : null);
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
     * Constructor for an instance containing a single range containing a single key.
     *
     * @param key key in range; must not be null
     * @throws IllegalArgumentException if {@code key} is null
     */
    public KeyRanges(byte[] key) {
        this(Collections.singletonList(new KeyRange(key)));
    }

    /**
     * Constructor for an instance containing a single range.
     *
     * @param min minimum key (inclusive); must not be null
     * @param max maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code min > max}
     */
    public KeyRanges(byte[] min, byte[] max) {
        this(Collections.singletonList(new KeyRange(min, max)));
    }

    /**
     * Construct an instance containing a single range corresponding to all keys with the given prefix.
     *
     * @param prefix prefix of all keys in the range
     * @return instance containing all keys prefixed by {@code prefix}
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public static KeyRanges forPrefix(byte[] prefix) {
        return new KeyRanges(KeyRange.forPrefix(prefix));
    }

// Instance methods

    /**
     * Get the {@link KeyRange}s underlying with this instance as a list.
     *
     * <p>
     * The returned {@link KeyRange}s will be listed in order.
     * </p>
     *
     * @return minimal, unmodifiable list of {@link KeyRange}s sorted by key range
     */
    public List<KeyRange> asList() {
        return Collections.unmodifiableList(this.ranges);
    }

    /**
     * Determine whether this instance is empty, i.e., contains no keys.
     *
     * @return true if this instance is empty
     */
    public boolean isEmpty() {
        return this.ranges.isEmpty();
    }

    /**
     * Determine whether this instance is "full", i.e., contains all keys.
     *
     * @return true if this instance is full
     */
    public boolean isFull() {
        return this.ranges.size() == 1 && this.ranges.get(0).isFull();
    }

    /**
     * Get the minimum key contained by this instance (inclusive).
     *
     * @return minimum key contained by this instance (inclusive), or null if this instance {@link #isEmpty}
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
     * Create a new instance from this one, with each {@link KeyRange} prefixed by the given byte sequence.
     *
     * @param prefix prefix to apply to this instance
     * @return prefixed instance
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public KeyRanges prefixedBy(final byte[] prefix) {
        return new KeyRanges(Lists.transform(this.ranges, new Function<KeyRange, KeyRange>() {
            @Override
            public KeyRange apply(KeyRange keyRange) {
                return keyRange.prefixedBy(prefix);
            }
        }));
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
        if (first.getMin().length == 0) {
            if ((lastMax = first.getMax()) == null) {
                assert this.ranges.size() == 1;
                return KeyRanges.EMPTY;
            }
            i++;
        } else
            lastMax = ByteUtil.EMPTY;
        while (true) {
            if (i == this.ranges.size()) {
                list.add(new KeyRange(lastMax, null));
                break;
            }
            final KeyRange next = this.ranges.get(i++);
            list.add(new KeyRange(lastMax, next.getMin()));
            if ((lastMax = next.getMax()) == null) {
                assert i == this.ranges.size();
                break;
            }
        }
        return new KeyRanges(list);
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
     * Determine whether this instance contains the given {@link KeyRange}, i.e., all keys contained by
     * the given {@link KeyRange} are also contained by this instance.
     *
     * @param range key range to test
     * @return true if this instance contains {@code range}, otherwise false
     * @throws IllegalArgumentException if {@code range} is null
     */
    public boolean contains(KeyRange range) {
        if (range == null)
            throw new IllegalArgumentException("null range");
        final KeyRange[] pair = this.findKey(range.getMin());
        if (pair[0] != pair[1])
            return false;
        final KeyRange container = pair[0];
        if (container == null)
            return false;
        return container.contains(range);
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
     * Return a new {@link KeyRanges} instance equal to this instance with all keys in the given {@link KeyRange} added.
     *
     * @param range range to add
     * @return this instance with {@code range} added
     * @throws IllegalArgumentException if {@code range} is null
     */
    public KeyRanges add(KeyRange range) {
        if (range == null)
            throw new IllegalArgumentException("null range");
        if (range.isEmpty())
            return this;
        final ArrayList<KeyRange> list = new ArrayList<>(this.ranges.size() + 1);
        list.addAll(this.ranges);
        list.add(range);
        return new KeyRanges(list);
    }

    /**
     * Return a new {@link KeyRanges} instance equal to this instance with all keys in the given {@link KeyRange} removed.
     *
     * @param range range to remove
     * @return this instance with {@code range} removed
     * @throws IllegalArgumentException if {@code range} is null
     */
    public KeyRanges remove(KeyRange range) {
        if (range == null)
            throw new IllegalArgumentException("null range");
        if (range.isEmpty())
            return this;
        return this.intersection(new KeyRanges(range).inverse());
    }

    /**
     * Create an instance that represents the union of this and the provided instance(s).
     *
     * @param others other instances
     * @return the union of this instance and {@code others}
     * @throws IllegalArgumentException if {@code others} or any element in {@code others} is null
     */
    public KeyRanges union(KeyRanges... others) {
        if (others == null)
            throw new IllegalArgumentException("null others");
        if (others.length == 0)
            return this;
        final ArrayList<KeyRange> list = new ArrayList<>(this.ranges.size() + others.length);
        list.addAll(this.ranges);
        for (KeyRanges other : others) {
            if (other == null)
                throw new IllegalArgumentException("null other");
            list.addAll(other.ranges);
        }
        return new KeyRanges(list);
    }

    /**
     * Create an instance that represents the intersection of this and the provided instance(s).
     *
     * @param others other instances
     * @return the intersection of this instance and {@code others}
     * @throws IllegalArgumentException if {@code others} or any element in {@code others} is null
     */
    public KeyRanges intersection(KeyRanges... others) {
        if (others == null)
            throw new IllegalArgumentException("null others");
        if (others.length == 0)
            return this;
        final ArrayList<KeyRange> list = new ArrayList<>(this.ranges.size() + others.length);
        list.addAll(this.inverse().ranges);
        for (KeyRanges other : others) {
            if (other == null)
                throw new IllegalArgumentException("null other");
            list.addAll(other.inverse().ranges);
        }
        return new KeyRanges(list).inverse();
    }

// Iterable<KeyRange>

    @Override
    public Iterator<KeyRange> iterator() {
        return this.asList().iterator();
    }

// SizeEstimating

    @Override
    public void addTo(SizeEstimator estimator) {
        estimator
          .addObjectOverhead()                              // this object overhead
          .addArrayListField(this.ranges)                   // this.ranges
          .addReferenceField();                             // this.lastContainingKeyRange (reference only)
        for (KeyRange range : this.ranges)
            estimator.add(range);
    }

// Serialization

    /**
     * Serialize this instance.
     *
     * @param out output
     * @throws IOException if an error occurs
     * @throws IllegalArgumentException if {@code out} is null
     */
    public void serialize(OutputStream out) throws IOException {
        UnsignedIntEncoder.write(out, this.ranges.size());
        byte[] prev = null;
        for (KeyRange range : this.ranges) {
            final byte[] min = range.getMin();
            final byte[] max = range.getMax();
            assert max != null || range == this.ranges.get(this.ranges.size() - 1);
            KeyListEncoder.write(out, min, prev);
            KeyListEncoder.write(out, max != null ? max : min, min);            // map final [min, null) to [min, min]
            prev = max;
        }
    }

    /**
     * Calculate the number of bytes required to serialize this instance via {@link #serialize serialize()}.
     *
     * @return number of serialized bytes
     */
    public long serializedLength() {
        long total = UnsignedIntEncoder.encodeLength(this.ranges.size());
        byte[] prev = null;
        for (KeyRange range : this.ranges) {
            final byte[] min = range.getMin();
            final byte[] max = range.getMax();
            total += KeyListEncoder.writeLength(min, prev);
            total += KeyListEncoder.writeLength(max != null ? max : min, min);
            prev = max;
        }
        return total;
    }

    /**
     * Deserialize an instance created by {@link #serialize serialize()}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @return deserialized instance
     * @throws IOException if an I/O error occurs
     * @throws java.io.EOFException if the input ends unexpectedly
     * @throws IllegalArgumentException if {@code input} is null
     */
    public static KeyRanges deserialize(InputStream input) throws IOException {
        if (input == null)
            throw new IllegalArgumentException("null input");
        final int count = UnsignedIntEncoder.read(input);
        final ArrayList<KeyRange> rangeList = new ArrayList<>(count);
        byte[] prev = null;
        for (int i = 0; i < count; i++) {
            final byte[] min = KeyListEncoder.read(input, prev);
            final byte[] max = KeyListEncoder.read(input, min);
            rangeList.add(new KeyRange(min, Arrays.equals(min, max) ? null : max));
            prev = max;
        }
        return new KeyRanges(rangeList);
    }

    /**
     * Deserialize an instance created by {@link #serialize serialize()} in the form of an
     * iterator of the individual {@link KeyRange}s.
     *
     * <p>
     * If an {@link IOException} is thrown while reading, the returned {@link Iterator}
     * will throw a {@link RuntimeException} wrapping it. If invalid data is encountered,
     * the returned {@link Iterator} will throw an {@link IllegalArgumentException}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @return deserialized iteration of {@link KeyRange}s
     * @throws IllegalArgumentException if {@code input} is null
     */
    public static Iterator<KeyRange> deserializeIterator(final InputStream input) {
        Preconditions.checkArgument(input != null, "null input");
        return new UnmodifiableIterator<KeyRange>() {

            private int remain = -1;
            private byte[] prev;

            @Override
            public boolean hasNext() {
                try {
                    this.init();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return this.remain > 0;
            }

            @Override
            public KeyRange next() {
                if (this.remain == 0)
                    throw new NoSuchElementException();
                final byte[] min;
                final byte[] max;
                try {
                    this.init();
                    min = KeyListEncoder.read(input, this.prev);
                    max = KeyListEncoder.read(input, min);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                final KeyRange range = new KeyRange(min, Arrays.equals(min, max) ? null : max);
                this.prev = max;
                this.remain--;
                return range;
            }

            private void init() throws IOException {
                if (this.remain == -1)
                    this.remain = UnsignedIntEncoder.read(input);
            }
        };
    }

// KeyFilter

    @Override
    public boolean contains(byte[] key) {
        final KeyRange[] pair = this.findKey(key);
        return pair[0] == pair[1] && pair[0] != null;
    }

    @Override
    public byte[] seekHigher(byte[] key) {
        final KeyRange[] pair = this.findKey(key);
        if (pair[0] == pair[1])
            return pair[0] != null ? key : null;
        return pair[1] != null ? pair[1].getMin() : null;
    }

    @Override
    public byte[] seekLower(byte[] key) {
        if (key == null)
            throw new IllegalArgumentException("null key");
        if (key.length == 0) {
            if (this.ranges.isEmpty())
                return null;
            final byte[] lastMax = this.ranges.get(this.ranges.size() - 1).getMax();
            return lastMax != null ? lastMax : ByteUtil.EMPTY;
        }
        final KeyRange[] pair = this.findKey(key);
        if (pair[0] == pair[1])
            return pair[0] != null ? key : null;
        return pair[0] != null ? pair[0].getMax() : null;
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

        // Create array
        final ArrayList<KeyRange> sortedRanges = new ArrayList<>(ranges);

        // Remove empty ranges
        for (Iterator<KeyRange> i = sortedRanges.iterator(); i.hasNext(); ) {
            if (i.next().isEmpty())
                i.remove();
        }

        // Sort remaining ranges by min, then max
        Collections.sort(sortedRanges, KeyRange.SORT_BY_MIN);

        // Consolidate
        final ArrayList<KeyRange> list = new ArrayList<>(ranges.size());
        KeyRange prev = null;
        for (KeyRange range : sortedRanges) {

            // Sanity check range
            if (range == null)
                throw new IllegalArgumentException("null range");

            // Handle first in list
            if (prev == null) {
                prev = range;
                continue;
            }

            // Compare to previous range
            final int diff1 = KeyRange.compare(range.getMin(), prev.getMin());
            assert diff1 >= 0;
            if (diff1 == 0) {                           // range contains prev -> discard prev
                assert range.contains(prev);
                prev = range;
                continue;
            }
            final int diff2 = KeyRange.compare(range.getMin(), prev.getMax());
            if (diff2 <= 0) {                           // prev and range overlap -> take their union
                final byte[] max = KeyRange.compare(range.getMax(), prev.getMax()) > 0 ? range.getMax() : prev.getMax();
                prev = new KeyRange(prev.getMin(), max);
                continue;
            }

            // OK add it
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

