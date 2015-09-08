
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a JSimpleDB model class method that should be invoked any time the associated model object is validated.
 *
 * <p>
 * This annotation does not change when an object will be enqueued for validation. It only affects the behavior once validation
 * of an instance is actually performed.
 *
 * <p>
 * When validating an object, validation via {@link Validate &#64;Validate} methods occurs after JSR 303 validation, if any.
 *
 * <p>
 * An optional list of validation {@link #groups} may be specified; if so, the annotated method will only be invoked when
 * one or more of specified validation groups (or a superclass thereof) is being validated. In particular, if {@link #groups}
 * is non-empty and does not contain a class that extends {@link javax.validation.groups.Default}, then the method will
 * not be invoked by automatic validation.
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and take zero parameters.
 * If validation fails, the method should throw a {@link org.jsimpledb.ValidationException}.
 *
 * @see org.jsimpledb.JTransaction#validate
 * @see org.jsimpledb.JObject#revalidate
 * @see org.jsimpledb.ValidationMode
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Validate {

    /**
     * Specify the validation group(s) for which the annotated method should be invoked.
     *
     * @return validation group(s) to use for validation; if empty, {@link javax.validation.groups.Default} is assumed
     */
    Class<?>[] groups() default {};
}

