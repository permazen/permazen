
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.Comparator;
import java.util.Objects;

/**
 * Represents a contiguous range of {@code byte[]} keys, when keys are sorted in unsigned lexical order.
 * Instances are defined by an inclusive lower bound and an exclusive upper bound.
 * The upper bound may be specified as null to represent no maximum.
 *
 * <p>
 * Instances are immutable: the minimum and maximum {@code byte[]} arrays are copied during
 * construction and when accessed by {@link #getMin} and {@link #getMax}.
 */
public class KeyRange {

    /**
     * The {@link KeyRange} containing the full range (i.e., all keys).
     */
    public static final KeyRange FULL = new KeyRange(ByteData.empty(), null);

    /**
     * Sorts instances by {@linkplain KeyRange#getMin min value}, then {@linkplain KeyRange#getMax max value}.
     */
    public static final Comparator<KeyRange> SORT_BY_MIN = Comparator
      .comparing(KeyRange::getMin)
      .thenComparing(KeyRange::getMax, KeyRange::compare);

    /**
     * Sorts instances by {@linkplain KeyRange#getMax max value}, then {@linkplain KeyRange#getMin min value}.
     */
    public static final Comparator<KeyRange> SORT_BY_MAX = Comparator
      .comparing(KeyRange::getMax, KeyRange::compare)
      .thenComparing(KeyRange::getMin);

    /**
     * Lower bound (inclusive), never null;
     */
    protected final ByteData min;

    /**
     * Upper bound (exclusive), or null for no maximum.
     */
    protected final ByteData max;

// Constructors

    /**
     * Constructor.
     *
     * @param min minimum key (inclusive); must not be null
     * @param max maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code min} is null
     * @throws IllegalArgumentException if {@code min > max}
     */
    public KeyRange(ByteData min, ByteData max) {
        Preconditions.checkArgument(min != null, "null min");
        if (KeyRange.compare(min, max) > 0) {
            throw new IllegalArgumentException(String.format(
              "min = %s > max = %s", ByteUtil.toString(min), ByteUtil.toString(max)));
        }
        this.min = min;
        this.max = max;
    }

    /**
     * Construct key range containing a single key.
     *
     * @param key the key contained in the range
     * @throws IllegalArgumentException if {@code key} is null
     */
    public KeyRange(ByteData key) {
        Preconditions.checkArgument(key != null, "null key");
        this.min = key;
        this.max = ByteUtil.getNextKey(this.min);
    }

    /**
     * Construct an instance containing all keys with the given prefix.
     *
     * @param prefix prefix of all keys in the range
     * @return range of keys prefixed by {@code prefix}
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public static KeyRange forPrefix(ByteData prefix) {
        Preconditions.checkArgument(prefix != null, "null prefix");
        if (prefix.isEmpty())
            return KeyRange.FULL;
        /*final*/ ByteData maxKey;
        try {
            maxKey = ByteUtil.getKeyAfterPrefix(prefix);
        } catch (IllegalArgumentException e) {
            maxKey = null;
        }
        return new KeyRange(prefix, maxKey);
    }

// Instance Methods

    /**
     * Get range minimum (inclusive).
     *
     * @return inclusivie minimum, never null
     */
    public ByteData getMin() {
        return this.min;
    }

    /**
     * Get range maximum (exclusive), or null if there is no upper bound.
     *
     * @return exclusivie maximum, or null for none
     */
    public ByteData getMax() {
        return this.max;
    }

    /**
     * Determine if this key range overlaps the specified key range, i.e., there exists at least one {@code byte[]}
     * key that both ranges have in common.
     *
     * @param range other instance
     * @return true if this instance overlaps {@code range}
     * @throws IllegalArgumentException if {@code range} is null
     */
    public boolean overlaps(KeyRange range) {
        Preconditions.checkArgument(range != null, "null range");
        return KeyRange.compare(this.min, range.max) < 0 && KeyRange.compare(range.min, this.max) < 0;
    }

    /**
     * Determine if this key range fully contains the specified key range.
     *
     * @param range other instance
     * @return true if this instance contains {@code range}
     * @throws IllegalArgumentException if {@code range} is null
     */
    public boolean contains(KeyRange range) {
        Preconditions.checkArgument(range != null, "null range");
        return KeyRange.compare(this.min, range.min) <= 0 && KeyRange.compare(this.max, range.max) >= 0;
    }

    /**
     * Determine if this key range contains the specified key.
     *
     * @param key key to test
     * @return true if this range contains {@code key}
     * @throws IllegalArgumentException if {@code key} is null
     */
    public boolean contains(ByteData key) {
        return this.compareTo(key) == 0;
    }

    /**
     * Determine whether this instance contains the full range covering all keys.
     *
     * @return true if this instance contains all keys
     */
    public boolean isFull() {
        return this.min.isEmpty() && this.max == null;
    }

    /**
     * Determine whether this instance contains exactly one key.
     *
     * <p>
     * If so, {@link #getMin} returns the key.
     *
     * @return true if this instance contains exactly one key, otherwise false
     */
    public boolean isSingleKey() {
        return this.max != null && ByteUtil.isConsecutive(this.min, this.max);
    }

    /**
     * Determine whether this instance contains all keys having some common prefix.
     *
     * <p>
     * If so, {@link #getMin} returns the prefix.
     *
     * @return true if this instance contains all keys having some common prefix, otherwise false
     */
    public boolean isPrefixRange() {
        final ByteData keyAfterPrefix;
        try {
            keyAfterPrefix = ByteUtil.getKeyAfterPrefix(this.min);
        } catch (IllegalArgumentException e) {
            return this.max == null;    // this.min is either empty or contains all 0xff bytes
        }
        return keyAfterPrefix.equals(this.max);
    }

    /**
     * Determine whether this instance contains zero keys (implying {@link #getMin}{@code == }{@link #getMax}).
     *
     * @return true if this instance contains no keys
     */
    public boolean isEmpty() {
        return this.min.equals(this.max);
    }

    /**
     * Create a new instance whose minimum and maximum keys are the same as this instance's
     * but with the given byte sequence prepended.
     *
     * @param prefix key range prefix
     * @return this range prefixed by {@code prefix}
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public KeyRange prefixedBy(ByteData prefix) {
        Preconditions.checkArgument(prefix != null, "null prefix");
        final ByteData prefixedMin = prefix.concat(this.min);
        /*final*/ ByteData prefixedMax;
        if (this.max != null)
            prefixedMax = prefix.concat(this.max);
        else {
            try {
                prefixedMax = ByteUtil.getKeyAfterPrefix(prefix);
            } catch (IllegalArgumentException e) {
                prefixedMax = null;
            }
        }
        return new KeyRange(prefixedMin, prefixedMax);
    }

    /**
     * Determine if this range is left of, contains, or is right of the given key.
     *
     * @param key key for comparison
     * @return -1 if this range is left of {@code key},
     *  0 if this range contains {@code key}, or 1 if this range is right of {@code key},
     * @throws IllegalArgumentException if {@code key} is null
     */
    public int compareTo(ByteData key) {
        Preconditions.checkArgument(key != null, "null key");
        if (KeyRange.compare(this.min, key) > 0)
            return 1;
        if (KeyRange.compare(this.max, key) <= 0)
            return -1;
        return 0;
    }

    /**
     * Compare two {@code byte[]} keys using unsigned lexical ordering, while also accepting
     * null values that represent "positive infinity".
     *
     * @param key1 first key, or null for "positive infinity"
     * @param key2 second key, or null for "positive infinity"
     * @return -1 if {@code key1 < key2}, 1 if {@code key1 > key2}, or zero if {@code key1 = key2}
     */
    public static int compare(ByteData key1, ByteData key2) {
        if (key1 == null && key2 == null)
            return 0;
        if (key1 == null)
            return 1;
        if (key2 == null)
            return -1;
        return key1.compareTo(key2);
    }

    /**
     * Create an empty key range at the specified key.
     *
     * @param key the minimum and maximum key
     * @return the empty key range {@code [key,key)}
     * @throws IllegalArgumentException if {@code key} is null
     */
    public static KeyRange empty(ByteData key) {
        return new KeyRange(key, key);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final KeyRange that = (KeyRange)obj;
        return this.min.equals(that.min) && Objects.equals(this.max, that.max);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ this.min.hashCode()
          ^ Objects.hashCode(this.max);
    }

    @Override
    public String toString() {
        if (this.isSingleKey())
            return "[" + ByteUtil.toString(this.min) + "]";
        if (this.isPrefixRange())
            return "[" + ByteUtil.toString(this.min) + "*]";
        if (this.max == null)
            return "[" + ByteUtil.toString(this.min) + ",)";
        return "[" + ByteUtil.toString(this.min) + "," + ByteUtil.toString(this.max) + ")";
    }
}
