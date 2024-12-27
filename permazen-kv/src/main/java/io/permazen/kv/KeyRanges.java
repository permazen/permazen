
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;

import io.permazen.kv.util.KeyListEncoder;
import io.permazen.util.ByteData;
import io.permazen.util.ImmutableNavigableSet;
import io.permazen.util.UnsignedIntEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * A fixed set of {@link KeyRange} instances that can be treated as a unified whole, in particular as a {@link KeyFilter}.
 *
 * <p>
 * Instances are not thread safe.
 *
 * @see KeyRange
 */
public class KeyRanges implements Iterable<KeyRange>, KeyFilter, Cloneable {

    private /*final*/ NavigableSet<KeyRange> ranges;

    private transient KeyRange lastContainingKeyRange;                      // used for optimization

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Creates an instance that contains all keys contained by any of the {@link KeyRange}s in {@code ranges}.
     * The given {@code ranges} may be empty, adjacent, overlap, and/or be listed in any order; this constructor
     * will normalize them.
     *
     * @param ranges individual key ranges
     * @throws IllegalArgumentException if {@code ranges} or any {@link KeyRange} therein is null
     */
    @SuppressWarnings("this-escape")
    public KeyRanges(Iterable<? extends KeyRange> ranges) {
        Preconditions.checkArgument(ranges != null, "null ranges");
        this.ranges = new TreeSet<>(KeyRange.SORT_BY_MIN);
        for (KeyRange range : ranges) {
            Preconditions.checkArgument(range != null, "null range");
            this.add(range);
        }
        assert this.checkMinimal();
    }

    /**
     * Constructor.
     *
     * <p>
     * Creates an instance that contains all keys contained by any of the {@link KeyRange}s in {@code ranges}.
     * The given {@code ranges} may be empty, adjacent, overlap, and/or be listed in any order; this constructor
     * will normalize them.
     *
     * @param ranges individual key ranges
     * @throws IllegalArgumentException if {@code ranges} or any {@link KeyRange} therein is null
     */
    @SuppressWarnings("this-escape")
    public KeyRanges(Stream<? extends KeyRange> ranges) {
        Preconditions.checkArgument(ranges != null, "null ranges");
        this.ranges = new TreeSet<>(KeyRange.SORT_BY_MIN);
        ranges.iterator().forEachRemaining(range -> {
            Preconditions.checkArgument(range != null, "null range");
            this.add(range);
        });
        assert this.checkMinimal();
    }

    /**
     * Constructor.
     *
     * <p>
     * Creates an instance that contains all keys contained by any of the {@link KeyRange}s in {@code ranges}.
     * The given {@code ranges} may be empty, adjacent, overlap, and/or be listed in any order; this constructor
     * will normalize them.
     *
     * @param ranges individual key ranges
     * @throws IllegalArgumentException if {@code ranges} or any {@link KeyRange} therein is null
     */
    public KeyRanges(KeyRange... ranges) {
        this(Arrays.asList(ranges));
    }

    /**
     * Copy constructor.
     *
     * @param ranges value to copy
     * @throws IllegalArgumentException if {@code ranges} is null
     */
    @SuppressWarnings("unchecked")
    public KeyRanges(KeyRanges ranges) {
        Preconditions.checkArgument(ranges != null, "null ranges");
        this.ranges = new TreeSet<>(ranges.ranges);
        this.lastContainingKeyRange = ranges.lastContainingKeyRange;
        assert this.checkMinimal();
    }

    /**
     * Constructor for an instance containing a single range.
     *
     * @param range single range
     * @throws IllegalArgumentException if {@code range} is null
     */
    public KeyRanges(KeyRange range) {
        this.ranges = new TreeSet<>(KeyRange.SORT_BY_MIN);
        if (!range.isEmpty())
            this.ranges.add(range);
        assert this.checkMinimal();
    }

    /**
     * Constructor for an instance containing a single range containing a single key.
     *
     * @param key key in range; must not be null
     * @throws IllegalArgumentException if {@code key} is null
     */
    public KeyRanges(ByteData key) {
        this.ranges = new TreeSet<>(KeyRange.SORT_BY_MIN);
        this.ranges.add(new KeyRange(key));
        assert this.checkMinimal();
    }

    /**
     * Constructor for an instance containing a single range.
     *
     * @param min minimum key (inclusive); must not be null
     * @param max maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code min > max}
     */
    public KeyRanges(ByteData min, ByteData max) {
        this(new KeyRange(min, max));
    }

    /**
     * Constructor to deserialize an instance created by {@link #serialize serialize()}.
     *
     * <p>
     * Equivalent to {@link #KeyRanges(InputStream, boolean) KeyRanges}{@code (input, false)}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @throws IOException if an I/O error occurs
     * @throws java.io.EOFException if the input ends unexpectedly
     * @throws IllegalArgumentException if {@code input} is null
     * @throws IllegalArgumentException if {@code input} is invalid
     */
    public KeyRanges(InputStream input) throws IOException {
        this(input, false);
    }

    /**
     * Constructor to deserialize an instance created by {@link #serialize serialize()}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @param immutable whether this new instance should be immutable
     * @throws IOException if an I/O error occurs
     * @throws java.io.EOFException if the input ends unexpectedly
     * @throws IllegalArgumentException if {@code input} is null
     * @throws IllegalArgumentException if {@code input} is invalid
     */
    public KeyRanges(InputStream input, boolean immutable) throws IOException {
        Preconditions.checkArgument(input != null, "null input");
        final int count = UnsignedIntEncoder.read(input);
        final KeyRange[] array = new KeyRange[count];
        ByteData prev = null;
        for (int i = 0; i < count; i++) {
            final ByteData min = KeyListEncoder.read(input, prev);
            final ByteData max = KeyListEncoder.read(input, min);
            Preconditions.checkArgument(prev == null || min.compareTo(prev) > 0, "invalid input");
            array[i] = new KeyRange(min, min.equals(max) ? null : max);         // map final [min, min) to [min, null]
            prev = max;
        }
        if (immutable)
            this.ranges = new ImmutableNavigableSet<>(array, KeyRange.SORT_BY_MIN);
        else {
            this.ranges = new TreeSet<>(KeyRange.SORT_BY_MIN);
            this.ranges.addAll(Arrays.asList(array));
        }
        assert this.checkMinimal();
    }

    private KeyRanges(NavigableSet<KeyRange> ranges) {
        assert ranges != null;
        this.ranges = ranges;
        assert this.checkMinimal();
    }

    /**
     * Construct an instance containing a single range corresponding to all keys with the given prefix.
     *
     * @param prefix prefix of all keys in the range
     * @return instance containing all keys prefixed by {@code prefix}
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public static KeyRanges forPrefix(ByteData prefix) {
        return new KeyRanges(KeyRange.forPrefix(prefix));
    }

    /**
     * Create an empty instance containing zero ranges.
     *
     * @return empty instance
     */
    public static KeyRanges empty() {
        return new KeyRanges();
    }

    /**
     * Create a "full" instance containing a single {@link KeyRange} that contains all keys.
     *
     * @return instance spanning all keys
     */
    public static KeyRanges full() {
        return new KeyRanges(KeyRange.FULL);
    }

// Instance methods

    /**
     * Get the {@link KeyRange}s underlying with this instance as a list.
     *
     * <p>
     * The returned {@link KeyRange}s will be listed in order. Modifications to the returned list do not affect this instance.
     *
     * @return minimal list of {@link KeyRange}s sorted by key range
     */
    public List<KeyRange> asList() {
        assert this.checkMinimal();
        return new ArrayList<>(this.ranges);
    }

    /**
     * Get a view of the {@link KeyRange}s underlying with this instance as a sorted set.
     *
     * <p>
     * The returned {@link KeyRange}s will be sorted in order according to {@link KeyRange#SORT_BY_MIN}.
     *
     * @return view of this instance as a minimal, unmodifiable sorted set of {@link KeyRange}s sorted by minimum key
     */
    public NavigableSet<KeyRange> asSet() {
        assert this.checkMinimal();
        return this.ranges instanceof ImmutableNavigableSet ?
          this.ranges : Collections.unmodifiableNavigableSet(this.ranges);
    }

    /**
     * Determine the number of individual {@link KeyRange}s contained in this instance.
     *
     * @return size of this instance
     */
    public int size() {
        assert this.checkMinimal();
        return this.ranges.size();
    }

    /**
     * Remove all keys from this instance.
     *
     * @throws UnsupportedOperationException if this instance is immutable
     */
    public void clear() {
        assert this.checkMinimal();
        this.ranges.clear();
    }

    /**
     * Determine whether this instance is empty, i.e., contains no keys.
     *
     * @return true if this instance is empty
     */
    public boolean isEmpty() {
        assert this.checkMinimal();
        return this.ranges.isEmpty();
    }

    /**
     * Determine whether this instance is "full", i.e., contains all keys.
     *
     * @return true if this instance is full
     */
    public boolean isFull() {
        assert this.checkMinimal();
        return !this.ranges.isEmpty() && this.ranges.first().isFull();
    }

    /**
     * Get the minimum key contained by this instance (inclusive).
     *
     * @return minimum key contained by this instance (inclusive), or null if this instance {@link #isEmpty}
     */
    public ByteData getMin() {
        assert this.checkMinimal();
        return !this.ranges.isEmpty() ? this.ranges.first().getMin() : null;
    }

    /**
     * Get the maximum key contained by this instance (exclusive).
     *
     * @return maximum key contained by this instance (exclusive),
     *  or null if there is no upper bound, or this instance {@link #isEmpty}
     */
    public ByteData getMax() {
        assert this.checkMinimal();
        return !this.ranges.isEmpty() ? this.ranges.last().getMax() : null;
    }

    /**
     * Create a new instance from this one, with each {@link KeyRange} prefixed by the given byte sequence.
     *
     * @param prefix prefix to apply to this instance
     * @return prefixed instance
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public KeyRanges prefixedBy(final ByteData prefix) {
        assert this.checkMinimal();
        Preconditions.checkArgument(prefix != null, "null prefix");
        return new KeyRanges(this.ranges.stream().map(range -> range.prefixedBy(prefix)));
    }

    /**
     * Create the inverse of this instance. The inverse contains all keys not contained by this instance.
     *
     * @return the inverse of this instance
     */
    public KeyRanges inverse() {
        assert this.checkMinimal();
        final Iterator<KeyRange> i = this.ranges.iterator();
        if (!i.hasNext())
            return KeyRanges.full();
        final TreeSet<KeyRange> inverseRanges = new TreeSet<>(KeyRange.SORT_BY_MIN);
        final KeyRange first = i.next();
        ByteData lastMax = first.max;
        if (!first.min.isEmpty())
            inverseRanges.add(new KeyRange(ByteData.empty(), first.min));
        while (lastMax != null) {
            if (!i.hasNext()) {
                inverseRanges.add(new KeyRange(lastMax, null));
                break;
            }
            final KeyRange next = i.next();
            inverseRanges.add(new KeyRange(lastMax, next.min));
            lastMax = next.max;
        }
        return new KeyRanges(inverseRanges);
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
        Preconditions.checkArgument(ranges != null, "null ranges");
        assert this.checkMinimal();
        for (KeyRange range : ranges.ranges) {
            if (!this.contains(range))
                return false;
        }
        return true;
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
        Preconditions.checkArgument(range != null, "null range");
        assert this.checkMinimal();
        final KeyRange[] neighbors = this.findKey(range.min);
        if (neighbors[0] != neighbors[1] || neighbors[0] == null)
            return false;
        final KeyRange match = neighbors[0];
        return match.contains(range);
    }

    /**
     * Determine whether this instance intersects the given {@link KeyRange}, i.e., there exists at least one key contained in both.
     *
     * @param range key range to test
     * @return true if this instance intersects {@code range}, otherwise false
     * @throws IllegalArgumentException if {@code range} is null
     */
    public boolean intersects(KeyRange range) {
        Preconditions.checkArgument(range != null, "null range");
        assert this.checkMinimal();

        // Get search key
        final KeyRange searchKey = new KeyRange(range.min, range.min);
        assert !this.ranges.contains(searchKey);

        // Check whether the next lower neighbor intersects the range
        final KeyRange lower = this.ranges.lower(searchKey);
        if (lower != null && KeyRange.compare(lower.max, range.min) > 0)
            return true;

        // Check whether the next higher neighbor intersects the range
        final KeyRange higher = this.ranges.higher(searchKey);
        if (higher != null && KeyRange.compare(higher.min, range.max) < 0)
            return true;

        // No intersection
        return false;
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
     *
     * @param key key to find
     * @return array with the containing {@link KeyRange} or nearest neighbor to the left (or null) and
     *  the containing {@link KeyRange} or nearest neighbor to the right (or null)
     * @throws IllegalArgumentException if {@code key} is null
     */
    public KeyRange[] findKey(ByteData key) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");
        assert this.checkMinimal();

        // Optimization: assume previous success is likely to repeat
        final KeyRange likelyKeyRange = this.lastContainingKeyRange;
        if (likelyKeyRange != null) {
            if (likelyKeyRange.contains(key) && this.ranges.contains(likelyKeyRange))
                return new KeyRange[] { likelyKeyRange, likelyKeyRange };
            this.lastContainingKeyRange = null;
        }

        // Check nearest neighbors
        final KeyRange searchKey = new KeyRange(key, key);
        assert !this.ranges.contains(searchKey);
        final KeyRange lower = this.ranges.lower(searchKey);
        if (lower != null) {
            if (lower.contains(key)) {
                this.lastContainingKeyRange = lower;
                return new KeyRange[] { lower, lower };
            }
        }
        final KeyRange higher = this.ranges.higher(searchKey);
        if (higher != null) {
            if (higher.contains(key)) {
                this.lastContainingKeyRange = higher;
                return new KeyRange[] { higher, higher };
            }
        }

        // Not contained
        return new KeyRange[] { lower, higher };
    }

    /**
     * Add all the keys in the given {@link KeyRange} to this instance.
     *
     * @param range key range to add
     * @throws IllegalArgumentException if {@code range} is null
     * @throws UnsupportedOperationException if this instance is immutable
     */
    public void add(KeyRange range) {

        // Sanity checks
        Preconditions.checkArgument(range != null, "null range");
        assert this.checkMinimal();

        // Handle trivial cases
        if (range.isEmpty())
            return;
        if (this.ranges.isEmpty()) {
            this.ranges.add(range);
            assert this.checkMinimal();
            return;
        }

        // Get search key
        final KeyRange searchKey = new KeyRange(range.min, range.min);
        assert !this.ranges.contains(searchKey);

        // Check for intersection with next lower range
        final KeyRange prev = this.ranges.lower(searchKey);
        if (prev != null) {

            // Check if 'prev' contains - or is adjacent to - 'range's min key
            if (KeyRange.compare(prev.max, range.min) >= 0) {

                // If 'prev' completely contains 'range' then we're done
                if (KeyRange.compare(prev.max, range.max) >= 0) {
                    assert this.checkMinimal();
                    return;
                }

                // Absorb 'prev' into 'range'
                this.ranges.remove(prev);
                range = new KeyRange(prev.min, range.max);
            }
        }

        // Check for intersection with higher ranges
        for (Iterator<KeyRange> i = this.ranges.tailSet(searchKey, false).iterator(); i.hasNext(); ) {
            final KeyRange next = i.next();

            // Does 'next' overlap or touch 'range'? If not, we're done looking
            if (KeyRange.compare(next.min, range.max) > 0)
                break;

            // Absorb 'next' into 'range'
            i.remove();
            if (KeyRange.compare(next.max, range.max) > 0) {
                range = new KeyRange(range.min, next.max);
                break;
            }
        }

        // Finally, add the new range
        this.ranges.add(range);
        assert this.checkMinimal();
    }

    /**
     * Remove all the keys in the given {@link KeyRange} from this instance.
     *
     * @param range range to remove
     * @throws IllegalArgumentException if {@code range} is null
     * @throws UnsupportedOperationException if this instance is immutable
     */
    public void remove(KeyRange range) {

        // Sanity checks
        Preconditions.checkArgument(range != null, "null range");
        assert this.checkMinimal();

        // Handle trivial cases
        if (range.isEmpty() || this.ranges.isEmpty())
            return;

        // Get search key
        final KeyRange searchKey = new KeyRange(range.min, range.min);
        assert !this.ranges.contains(searchKey);

        // Check for intersection with next lower range
        final KeyRange prev = this.ranges.lower(searchKey);
        if (prev != null && prev.contains(range.min)) {     // if 'prev' contains 'range's min key, subtract 'range' from 'prev'
            this.ranges.remove(prev);
            if (KeyRange.compare(prev.min, range.min) < 0)
                this.ranges.add(new KeyRange(prev.min, range.min));
            if (KeyRange.compare(prev.max, range.max) > 0) {
                this.ranges.add(new KeyRange(range.max, prev.max));
                assert this.checkMinimal();
                return;
            }
        }

        // Check for intersection with higher ranges
        for (Iterator<KeyRange> i = this.ranges.tailSet(searchKey, false).iterator(); i.hasNext(); ) {
            final KeyRange next = i.next();

            // Does 'next' overlap 'range'? If not, we're done looking
            if (KeyRange.compare(next.min, range.max) >= 0)
                break;

            // Remove 'next'
            i.remove();

            // If 'range' wholly contains 'next', continue looking
            if (KeyRange.compare(next.max, range.max) <= 0)
                continue;

            // Replace 'next' with a truncated version
            this.ranges.add(new KeyRange(range.max, next.max));
            break;
        }

        // Done
        assert this.checkMinimal();
    }

    /**
     * Remove all the keys not also in the given {@link KeyRange} from this instance.
     *
     * @param range key range to intersect with
     * @throws IllegalArgumentException if {@code range} is null
     * @throws UnsupportedOperationException if this instance is immutable
     */
    public void intersect(KeyRange range) {
        this.intersect(new KeyRanges(range));
    }

    /**
     * Add all the key ranges in the given {@link KeyRanges} to this instance.
     *
     * @param ranges key ranges to add
     * @throws IllegalArgumentException if {@code ranges} is null
     * @throws UnsupportedOperationException if this instance is immutable
     */
    @SuppressWarnings("unchecked")
    public void add(KeyRanges ranges) {
        Preconditions.checkArgument(ranges != null, "null ranges");
        assert this.checkMinimal();
        ranges.ranges.forEach(this::add);
    }

    /**
     * Remove all the key ranges in the given {@link KeyRanges} from this instance.
     *
     * @param ranges key ranges to remove
     * @throws IllegalArgumentException if {@code ranges} is null
     */
    public void remove(KeyRanges ranges) {
        Preconditions.checkArgument(ranges != null, "null ranges");
        assert this.checkMinimal();
        if (this.ranges.isEmpty())
            return;
        ranges.ranges.forEach(this::remove);
    }

    /**
     * Remove all key ranges not also in the given {@link KeyRanges} from this instance.
     *
     * <p>
     * Equivalent to {@code remove(ranges.inverse())}.
     *
     * @param ranges key ranges to intersect with
     * @throws IllegalArgumentException if {@code ranges} is null
     * @throws UnsupportedOperationException if this instance is immutable
     */
    public void intersect(KeyRanges ranges) {
        this.remove(ranges.inverse());
    }

// Iterable<KeyRange>

    @Override
    public Iterator<KeyRange> iterator() {
        return this.asSet().iterator();
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
        assert this.checkMinimal();
        UnsignedIntEncoder.write(out, this.ranges.size());
        ByteData prev = null;
        for (KeyRange range : this.ranges) {
            final ByteData min = range.min;
            final ByteData max = range.max;
            assert max != null || range == this.ranges.last();
            KeyListEncoder.write(out, min, prev);
            KeyListEncoder.write(out, max != null ? max : min, min);            // map final [min, null) to [min, min]
            prev = max;
        }
    }

    /**
     * Calculate the number of bytes required to serialize this instance via {@link #serialize serialize()}.
     *
     * @return number of serialized bytes
     * @throws IllegalArgumentException if the total exceeds {@link Long#MAX_VALUE}
     */
    public long serializedLength() {
        long total = UnsignedIntEncoder.encodeLength(this.ranges.size());
        ByteData prev = null;
        for (KeyRange range : this.ranges) {
            final ByteData min = range.min;
            final ByteData max = range.max;
            total += KeyListEncoder.writeLength(min, prev);
            total += KeyListEncoder.writeLength(max != null ? max : min, min);
            if (total < 0)
                throw new IllegalArgumentException("total is too large");
            prev = max;
        }
        return total;
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
            private ByteData prev;

            @Override
            public boolean hasNext() {
                this.init();
                return this.remain > 0;
            }

            @Override
            public KeyRange next() {
                this.init();
                if (this.remain == 0)
                    throw new NoSuchElementException();
                final ByteData min;
                final ByteData max;
                try {
                    min = KeyListEncoder.read(input, this.prev);
                    max = KeyListEncoder.read(input, min);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (this.prev != null && min.compareTo(prev) <= 0)
                    throw new IllegalArgumentException("mis-ordered keys");
                final KeyRange range = new KeyRange(min, min.equals(max) ? null : max);
                this.prev = max;
                this.remain--;
                return range;
            }

            private void init() {
                if (this.remain == -1) {
                    try {
                        this.remain = UnsignedIntEncoder.read(input);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

// KeyFilter

    @Override
    public boolean contains(ByteData key) {
        assert this.checkMinimal();
        final KeyRange[] neighbors = this.findKey(key);
        return neighbors[0] == neighbors[1] && neighbors[0] != null;
    }

    @Override
    public ByteData seekHigher(ByteData key) {
        assert this.checkMinimal();
        final KeyRange[] neighbors = this.findKey(key);
        if (neighbors[0] == neighbors[1])
            return neighbors[0] != null ? key : null;
        return neighbors[1] != null ? neighbors[1].getMin() : null;
    }

    @Override
    public ByteData seekLower(ByteData key) {
        Preconditions.checkArgument(key != null, "null key");
        assert this.checkMinimal();
        if (key.isEmpty()) {
            if (this.ranges.isEmpty())
                return null;
            final ByteData lastMax = this.ranges.last().getMax();
            return lastMax != null ? lastMax : ByteData.empty();
        }
        final KeyRange[] neighbors = this.findKey(key);
        if (neighbors[0] == neighbors[1])
            return neighbors[0] != null ? key : null;
        return neighbors[0] != null ? neighbors[0].getMax() : null;
    }

// Cloneable

    /**
     * Clone this instance.
     *
     * <p>
     * The returned clone will always be mutable, even if this instance is not.
     */
    @Override
    @SuppressWarnings("unchecked")
    public KeyRanges clone() {
        assert this.checkMinimal();
        final KeyRanges clone;
        try {
            clone = (KeyRanges)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.ranges = new TreeSet<>(clone.ranges);
        assert clone.checkMinimal();
        return clone;
    }

    /**
     * Return an immutable snapshot of this instance.
     *
     * @return immutable snapshot
     */
    public KeyRanges readOnlySnapshot() {
        if (this.ranges instanceof ImmutableNavigableSet)
            return this;
        return new KeyRanges(new ImmutableNavigableSet<>(this.ranges));
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
        return this.getClass().hashCode() ^ this.ranges.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append('[');
        int index = 0;
    rangeLoop:
        for (KeyRange range : this.ranges) {
            switch (index++) {
            case 0:
                break;
            case 32:
                buf.append("...");
                break rangeLoop;
            default:
                buf.append(",");
                break;
            }
            buf.append(range);
        }
        buf.append(']');
        return buf.toString();
    }

// Internal methods

    private boolean checkMinimal() {
        KeyRange prev = null;
        for (KeyRange range : this.ranges) {
            assert !range.isEmpty() : "contains empty range: " + range;
            assert prev == null || KeyRange.compare(prev.max, range.min) < 0 : "touching ranges: " + prev + ", " + range;
            prev = range;
        }
        return true;
    }
}
