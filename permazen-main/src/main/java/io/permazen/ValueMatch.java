
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.Values;
import io.permazen.encoding.Encoding;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Defines a predicate which matches certain (core API) values for a simple field.
 */
final class ValueMatch<T> {

    private final Encoding<T> encoding;
    private final boolean nulls;                // true to match the null value (if it exists)
    private final boolean nonNulls;             // true to match all non-null values
    private final ArrayList<T> values;          // non-null, core API values, sorted by this.encoding

    ValueMatch(Encoding<T> encoding, Values values) {

        // Sanity check
        Preconditions.checkArgument(encoding != null, "null encoding");
        Preconditions.checkArgument(values != null, "null values");

        // Initialize
        this.encoding = encoding;
        this.nulls = values.nulls();
        this.nonNulls = values.nonNulls();
        this.values = new ArrayList<>(values.value().length);

        // Parse values
        for (String string : values.value()) {
            final T value = encoding.fromString(string);
            if (value == null)
                throw new RuntimeException("internal error: Encoding.fromString() returned null value");
            this.values.add(value);
        }

        // Sort values
        Collections.sort(this.values, this.encoding);
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
        return this.nonNulls && (this.nulls || !this.encoding.allowsNull());
    }

    public boolean neverMatches() {
        return this.values.isEmpty() && !this.nonNulls && (!this.nulls || !this.encoding.allowsNull());
    }

    @SuppressWarnings("unchecked")
    public boolean matches(Object value) {
        if (value == null)
            return this.nulls;
        if (this.nonNulls)
            return true;
        return Collections.binarySearch(this.values, (T)value, this.encoding) >= 0;
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
        return values.value().length == 0 && !values.nonNulls() && !values.nulls();
    }

    @Override
    public String toString() {
        return "ValueMatch"
          + "[nulls=" + this.nulls
          + ",nonNulls=" + this.nonNulls
          + ",values=" + this.values
          + "]";
    }
}
