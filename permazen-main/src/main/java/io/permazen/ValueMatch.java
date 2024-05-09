
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.ValueRange;
import io.permazen.annotation.Values;
import io.permazen.encoding.Encoding;
import io.permazen.util.BoundType;
import io.permazen.util.Bounds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Defines a predicate which matches certain (core API) values for a simple field.
 */
final class ValueMatch<T> {

    private final Encoding<T> encoding;
    private final boolean nulls;                // true to match the null value (if it exists)
    private final boolean nonNulls;             // true to match all non-null values
    private final ArrayList<T> values;          // non-null, core API values, sorted by this.encoding
    private final ArrayList<Bounds<T>> ranges;  // core API value ranges, with min() sorted by this.encoding

    ValueMatch(Encoding<T> encoding, Values values) {

        // Sanity check
        Preconditions.checkArgument(encoding != null, "null encoding");
        Preconditions.checkArgument(values != null, "null values");

        // Initialize
        this.encoding = encoding;
        this.nulls = values.nulls();
        this.nonNulls = values.nonNulls();
        this.values = new ArrayList<>(values.value().length);
        this.ranges = new ArrayList<>(values.ranges().length);

        // Parse values
        for (String string : values.value())
            this.values.add(this.parseValue(string));

        // Parse ranges
        for (ValueRange range : values.ranges()) {

            // Build bounds
            Bounds<T> bounds = new Bounds<>();
            if (!range.min().isEmpty()) {
                bounds = bounds.withLowerBound(this.parseValue(range.min()),
                  range.inclusiveMin() ? BoundType.INCLUSIVE : BoundType.EXCLUSIVE);
            }
            if (!range.max().isEmpty()) {
                bounds = bounds.withUpperBound(this.parseValue(range.max()),
                  range.inclusiveMax() ? BoundType.INCLUSIVE : BoundType.EXCLUSIVE);
            }

            // Sanity check
            if (bounds.isEmpty(this.encoding)) {
                throw new IllegalArgumentException(String.format(
                  "value range \"%s\" to \"%s\" matches nothing", range.min(), range.max()));
            }

            // Consolidate with any existing ranges
            Bounds<T> combined = null;
            for (int i = 0; i < this.ranges.size(); i++) {
                final Bounds<T> existingBounds = this.ranges.get(i);
                if ((combined = existingBounds.union(this.encoding, bounds)) != null) {
                    this.ranges.set(i, combined);
                    break;
                }
            }
            if (combined == null)
                this.ranges.add(bounds);
        }

        // Sort values
        Collections.sort(this.values, this.encoding);
    }

    private T parseValue(String string) {
        final T value = this.encoding.fromString(string);
        if (value == null)
            throw new RuntimeException("internal error: Encoding.fromString() returned null value");
        return value;
    }

    /**
     * Validate this instance.
     *
     * @param composite true if part of a composite uniqueness constraint
     */
    public void validate(boolean composite) {
        if (this.nonNulls && !this.values.isEmpty())
            throw new IllegalArgumentException("explicit values are redundant with nonNulls() = true");
        if (this.neverMatches())
            throw new IllegalArgumentException("annotation will never match");
        if (!composite && this.alwaysMatches())
            throw new IllegalArgumentException("annotation will always match");
    }

    public boolean alwaysMatches() {
        return this.nonNulls && (this.nulls || !this.encoding.supportsNull());
    }

    public boolean neverMatches() {
        return this.values.isEmpty()
          && this.ranges.isEmpty()
          && !this.nonNulls
          && (!this.nulls || !this.encoding.supportsNull());
    }

    @SuppressWarnings("unchecked")
    public boolean matches(Object value) {

        // Check null
        if (value == null)
            return this.nulls;

        // Check non-null
        if (this.nonNulls)
            return true;

        // Check individual values
        if (Collections.binarySearch(this.values, (T)value, this.encoding) >= 0)
            return true;

        // Check value ranges
        for (Bounds<T> range : this.ranges) {
            if (range.isWithinBounds(this.encoding, (T)value))
                return true;
        }

        // No match
        return false;
    }

    /**
     * Create a new instance, also binding the generic type.
     */
    public static <T> ValueMatch<T> create(Encoding<T> encoding, Values values) {
        return new ValueMatch<>(encoding, values);
    }

    /**
     * Is the given {@link Values &#64;Values} annotation equal to its "default value" (i.e., empty)?
     */
    public static boolean isEmpty(Values values) {
        Preconditions.checkArgument(values != null, "null values");
        return values.value().length == 0 && values.ranges().length == 0 && !values.nonNulls() && !values.nulls();
    }

// Object

    @Override
    public String toString() {
        return "ValueMatch"
          + "[nulls=" + this.nulls
          + ",nonNulls=" + this.nonNulls
          + ",values=" + this.values
          + ",ranges=" + this.ranges
          + "]";
    }

    @Override
    public int hashCode() {
        return this.encoding.hashCode()
          ^ (Boolean.hashCode(this.nulls) << 5)
          ^ (Boolean.hashCode(this.nonNulls) << 7)
          ^ this.values.stream().mapToInt(Objects::hashCode).reduce(0, (x, y) -> x ^ y)
          ^ this.ranges.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ValueMatch<?> that = (ValueMatch<?>)obj;
        return this.encoding.equals(that.encoding)
          && this.nulls == that.nulls
          && this.nonNulls == that.nonNulls
          && this.values.size() == that.values.size()
          && IntStream.range(0, this.values.size()).allMatch(i -> Objects.equals(this.values.get(i), that.values.get(i)))
          && this.ranges.equals(that.ranges);
    }
}
