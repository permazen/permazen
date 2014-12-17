
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import com.google.common.primitives.Bytes;

import java.util.Arrays;
import java.util.Comparator;

import org.jsimpledb.util.ByteUtil;

/**
 * Represents a range of {@code byte[]} keys sorted in unsigned lexical order.
 * Either bound may be specified as null to represent no minimum or no maximum (respectively).
 *
 * <p>
 * Instances are immutable: the minimum and maximum {@code byte[]} arrays are copied during
 * construction and when accessed by {@link #getMin} and {@link #getMax}.
 * </p>
 */
public class KeyRange {

    /**
     * The {@link KeyRange} containing the full range (i.e., all keys).
     */
    public static final KeyRange FULL = new KeyRange(null, null);

    /**
     * Constant used by {@link #compare KeyRange.compare()} to distinguish null values.
     */
    public static final boolean MIN = false;

    /**
     * Constant used by {@link #compare KeyRange.compare()} to distinguish null values.
     */
    public static final boolean MAX = true;

    /**
     * Sorts instances by {@linkplain KeyRange#getMin min value}, then {@linkplain KeyRange#getMax max value}.
     */
    public static final Comparator<KeyRange> SORT_BY_MIN = new Comparator<KeyRange>() {
        @Override
        public int compare(KeyRange keyRange1, KeyRange keyRange2) {
            int diff = KeyRange.compare(keyRange1.min, KeyRange.MIN, keyRange2.min, KeyRange.MIN);
            if (diff != 0)
                return diff;
            diff = KeyRange.compare(keyRange1.max, KeyRange.MAX, keyRange2.max, KeyRange.MAX);
            if (diff != 0)
                return diff;
            return 0;
        }
    };

    /**
     * Sorts instances by {@linkplain KeyRange#getMax max value}, then {@linkplain KeyRange#getMin min value}.
     */
    public static final Comparator<KeyRange> SORT_BY_MAX = new Comparator<KeyRange>() {
        @Override
        public int compare(KeyRange keyRange1, KeyRange keyRange2) {
            int diff = KeyRange.compare(keyRange1.max, KeyRange.MAX, keyRange2.max, KeyRange.MAX);
            if (diff != 0)
                return diff;
            diff = KeyRange.compare(keyRange1.min, KeyRange.MIN, keyRange2.min, KeyRange.MIN);
            if (diff != 0)
                return diff;
            return 0;
        }
    };

    /**
     * Lower bound (inclusive), or null for no minimum. Subclasses must <b>not</b> modify the array (to preserve immutability).
     */
    protected final byte[] min;

    /**
     * Upper bound (exclusive), or null for no maximum. Subclasses must <b>not</b> modify the array (to preserve immutability).
     */
    protected final byte[] max;

// Constructors

    /**
     * Constructor.
     *
     * @param min minimum key (inclusive), or null for no minimum
     * @param max maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code min > max}
     */
    public KeyRange(byte[] min, byte[] max) {
        if (KeyRange.compare(min, KeyRange.MIN, max, KeyRange.MAX) > 0)
            throw new IllegalArgumentException("min = " + ByteUtil.toString(min) + " > max = " + ByteUtil.toString(max));
        this.min = min == null ? null : min.clone();
        this.max = max == null ? null : max.clone();
    }

    /**
     * Construct key range containing a single key.
     *
     * @param key the key contained in the range
     * @throws IllegalArgumentException if {@code key} is null
     */
    public KeyRange(byte[] key) {
        if (key == null)
            throw new IllegalArgumentException("null key");
        this.min = key.clone();
        this.max = ByteUtil.getNextKey(this.min);
    }

    /**
     * Construct an instance containing all keys with the given prefix.
     *
     * @param prefix prefix of all keys in the range
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public static KeyRange forPrefix(byte[] prefix) {
        if (prefix == null)
            throw new IllegalArgumentException("null prefix");
        if (prefix.length == 0)
            return new KeyRange(null, null);
        /*final*/ byte[] maxKey;
        try {
            maxKey = ByteUtil.getKeyAfterPrefix(prefix);
        } catch (IllegalArgumentException e) {
            maxKey = null;
        }
        return new KeyRange(prefix, maxKey);
    }

// Instance Methods

    /**
     * Get range minimum (inclusive), or null if there is no lower bound.
     */
    public byte[] getMin() {
        return this.min == null ? null : this.min.clone();
    }

    /**
     * Get range maximum (exclusive), or null if there is no upper bound.
     */
    public byte[] getMax() {
        return this.max == null ? null : this.max.clone();
    }

    /**
     * Determine if this key range overlaps the specified key range.
     *
     * @param range other instance
     * @throws IllegalArgumentException if {@code range} is null
     */
    public boolean overlaps(KeyRange range) {
        if (range == null)
            throw new IllegalArgumentException("null range");
        return KeyRange.compare(this.min, MIN, range.max, MAX) < 0 && KeyRange.compare(range.min, MIN, this.max, MAX) < 0;
    }

    /**
     * Determine if this key range fully contains the specified key range.
     *
     * @param range other instance
     * @throws IllegalArgumentException if {@code range} is null
     */
    public boolean contains(KeyRange range) {
        if (range == null)
            throw new IllegalArgumentException("null range");
        return KeyRange.compare(this.min, MIN, range.min, MIN) <= 0 && KeyRange.compare(this.max, MAX, range.max, MAX) >= 0;
    }

    /**
     * Determine if this key range contains the specified key.
     *
     * @throws IllegalArgumentException if {@code key} is null
     */
    public boolean contains(byte[] key) {
        return this.compareTo(key) == 0;
    }

    /**
     * Determine whether this instance contains the full range covering all keys.
     */
    public boolean isFull() {
        return this.min == null && this.max == null;
    }

    /**
     * Determine whether this instance contains zero keys (implying {@link #getMin}{@code == }{@link #getMax}).
     */
    public boolean isEmpty() {
        return this.min != null && this.max != null && ByteUtil.compare(this.min, this.max) == 0;
    }

    /**
     * Create a new instance whose minimum and maximum keys are the same as this instance's
     * but with the given byte sequence prepended.
     *
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public KeyRange prefixedBy(byte[] prefix) {
        if (prefix == null)
            throw new IllegalArgumentException("null prefix");
        final byte[] prefixedMin = this.min != null ? Bytes.concat(prefix, this.min) : prefix;
        final byte[] prefixedMax = this.max != null ? Bytes.concat(prefix, this.max) : ByteUtil.getNextKey(prefix);
        return new KeyRange(prefixedMin, prefixedMax);
    }

    /**
     * Determine if this range is left of, contains, or is right of the given key.
     *
     * @return -1 if this range is left of {@code key},
     *  0 if this range contains {@code key}, or 1 if this range is right of {@code key},
     * @throws IllegalArgumentException if {@code key} is null
     */
    public int compareTo(byte[] key) {
        if (key == null)
            throw new IllegalArgumentException("null key");
        if (KeyRange.compare(this.min, MIN, key, MIN) > 0)
            return 1;
        if (KeyRange.compare(this.max, MAX, key, MAX) <= 0)
            return -1;
        return 0;
    }

    /**
     * Compare two {@code byte[]} keys using unsigned lexical ordering, while also accepting
     * null values that represent "negative infinity" and "positive infinity".
     *
     * @param range1 first key
     * @param type1 how to interpret {@code range1} if value is null:
     *  {@link #MIN} (for "negative infinity") or {@link #MAX} (for "positive infinity")
     * @param range2 second key
     * @param type2 how to interpret {@code range2} if value is null:
     *  {@link #MIN} (for "negative infinity") or {@link #MAX} (for "positive infinity")
     */
    public static int compare(byte[] range1, boolean type1, byte[] range2, boolean type2) {
        if (range1 == null && range2 == null)
            return type1 == type2 ? 0 : type1 == MIN ? -1 : 1;
        if (range1 == null)
            return type1 == MIN ? -1 : 1;
        if (range2 == null)
            return type2 == MAX ? -1 : 1;
        return ByteUtil.compare(range1, range2);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final KeyRange that = (KeyRange)obj;
        return (this.min == null ? that.min == null : Arrays.equals(this.min, that.min))
          && (this.max == null ? that.max == null : Arrays.equals(this.max, that.max));
    }

    @Override
    public int hashCode() {
        return (this.min != null ? Arrays.hashCode(this.min) : 0)
          ^ (this.max != null ? Arrays.hashCode(this.max) : 0);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + ByteUtil.toString(this.min) + "," + ByteUtil.toString(this.max) + "]";
    }
}

