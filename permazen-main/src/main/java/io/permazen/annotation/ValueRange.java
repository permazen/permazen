
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.encoding.Encoding;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines a predicate that matches a contiguous range of values of a simple field.
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ValueRange {

    /**
     * Get the beginning of the value range.
     *
     * <p>
     * The value must be a valid {@link String} encoding of the associated field, acceptable to
     * {@link Encoding#fromString Encoding.fromString()}.
     *
     * @return value range minimum, or empty string for no minimum
     */
    String min() default "";

    /**
     * Get the end of the value range.
     *
     * <p>
     * The value must be a valid {@link String} encoding of the associated field, acceptable to
     * {@link Encoding#fromString Encoding.fromString()}.
     *
     * @return value range maximum, or empty string for no maximum
     */
    String max() default "";

    /**
     * Get whether the {@link #min} is an inclusive or exclusive lower bound.
     *
     * @return true if lower bound is inclusive, false if exclusive
     */
    boolean inclusiveMin() default true;

    /**
     * Get whether the {@link #max} is an inclusive or exclusive upper bound.
     *
     * @return true if upper bound is inclusive, false if exclusive
     */
    boolean inclusiveMax() default false;
}
