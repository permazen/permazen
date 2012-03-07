
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;

/**
 * Works like the standard {@link javax.validation.constraints.Pattern @Pattern} but applies to any
 * type of object, converting to {@link String} as necessary via {@link Object#toString}, and recursing
 * on collection types.
 */
@Documented
@Constraint(validatedBy = PatternValidator.class)
@Target({ ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@ReportAsSingleViolation
public @interface Pattern {

    String message() default "Does not match the pattern \"{regexp}\"";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    /**
     * Regular expression that must be matched.
     */
    String regexp();

    /**
     * Regular expression flags.
     */
    javax.validation.constraints.Pattern.Flag[] flags() default {};
}

