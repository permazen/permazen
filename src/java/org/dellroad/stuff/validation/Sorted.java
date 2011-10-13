
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
import java.util.Comparator;

import javax.validation.Constraint;
import javax.validation.Payload;

/**
 * Validation constraint that checks elements are sorted.
 * Applies to non-primitive arrays, collections and maps; for maps, the keys are examined.
 * If any element is null, it is skipped.
 */
@Documented
@Constraint(validatedBy = SortedValidator.class)
@Target({ ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface Sorted {

    String message() default "Collection is not sorted";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    /**
     * Specifies a {@link java.util.Comparator} to use. If none is specified, the natural sort ordering is used.
     * The class must have a default constructor.
     */
    Class<? extends Comparator> comparator() default Comparator.class;

    /**
     * Configures whether the sorting should be strict, i.e., whether adjacent equal elements should be disallowed.
     */
    boolean strict() default true;
}

