
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
 * Validation constraint requiring a {@link String} to be either a valid fully-qualified DNS hostname
 * or an IPv4 address.
 */
@Documented
@Constraint(validatedBy = {})
@Target({ ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Pattern(regexp =
    "^((([\\p{Alnum}]([-\\p{Alnum}]*[\\p{Alnum}])?)\\.)+([\\p{Alpha}]([-\\p{Alnum}]*[\\p{Alnum}])?))"
  + "|("
    + "([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])" + "\\."
    + "([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])" + "\\."
    + "([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])" + "\\."
    + "([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])"
  + ")$")
@ReportAsSingleViolation
public @interface HostOrIP {

    String message() default "Not a valid hostname or IPv4 address";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}

