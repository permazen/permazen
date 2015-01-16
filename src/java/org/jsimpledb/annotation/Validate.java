
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a JSimpleDB model class method that should be invoked any time the associated model object is validated.
 *
 * <p>
 * This annotation does not change when an object will be enqueued for validation. It only affects the behavior once validation
 * of an instance is actually performed.
 * </p>
 *
 * <p>
 * When validating an object, validation via {@link Validate &#64;Validate} methods occurs after JSR 303 validation, if any.
 * </p>
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and take zero parameters.
 * If validation fails, the method should throw a {@link org.jsimpledb.ValidationException}.
 * </p>
 *
 * @see org.jsimpledb.JTransaction#validate
 * @see org.jsimpledb.JObject#revalidate
 * @see org.jsimpledb.ValidationMode
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Validate {
}

