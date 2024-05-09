
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.encoding.Encoding;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines a predicate that matches certain values of a simple field.
 *
 * <p>
 * A field value matches if it matches any of {@link #nulls}, {@link #nonNulls}, {@link #ranges} or {@link #value}
 * properties match the value.
 *
 * <p>
 * To match all field values, combine {@link #nulls} with {@link #nonNulls}. For primitive field types,
 * {@link #nulls} is irrelevant and ignored, so {@link #nonNulls} by itself matches all values.
 *
 * <p>
 * It is an error to combine {@link #nonNulls} with {@link #value}, as that would be redundant.
 *
 * @see PermazenField#uniqueExcludes
 * @see PermazenCompositeIndex#uniqueExcludes
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Values {

    /**
     * Specify whether the null value should match.
     */
    boolean nulls() default false;

    /**
     * Specify whether any non-null value should match.
     */
    boolean nonNulls() default false;

    /**
     * Enumerate specific field value(s) that should match.
     *
     * <p>
     * Each value must be a valid {@link String} encoding of the associated field, acceptable to
     * {@link Encoding#fromString Encoding.fromString()}.
     *
     * @return specific matching field values
     */
    String[] value() default {};

    /**
     * Specify range(s) of field values that should match.
     *
     * @return specific matching field value ranges
     */
    ValueRange[] ranges() default {};
}
