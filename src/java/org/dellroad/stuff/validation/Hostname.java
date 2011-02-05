
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
import javax.validation.constraints.Pattern;

/**
 * Validation constraint requiring a {@link String} to be a valid DNS hostname (or hostname component).
 */
@Documented
@Constraint(validatedBy = {})
@Target({ ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Pattern(regexp = "^(([\\p{Alnum}]([-\\p{Alnum}]*[\\p{Alnum}])?)\\.)*([\\p{Alpha}]([-\\p{Alnum}]*[\\p{Alnum}])?)$")
@ReportAsSingleViolation
public @interface Hostname {

    String message() default "Invalid hostname or hostname component";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}

