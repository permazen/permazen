
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.ValidationException;
import io.permazen.ValidationMode;

import jakarta.validation.groups.Default;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a Permazen model class method that should be invoked any time the associated model object is validated.
 *
 * <p>
 * This annotation does not change when an object will be enqueued for validation. It only affects the behavior once validation
 * of an instance is actually performed.
 *
 * <p>
 * An optional list of validation {@link #groups} may be specified; if so, the annotated method will only be invoked when
 * one or more of specified validation groups (or a superclass thereof) is being validated. In particular, if {@link #groups}
 * is non-empty and does not contain a class that extends {@link Default}, then the method will
 * not be invoked by automatic validation.
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and take zero parameters.
 * If validation fails, the method should throw a {@link ValidationException}.
 *
 * <p><b>"Fixup" Validations</b></p>
 *
 * <p>
 * Annotated methods are not restricted from performing additional mutating operations in the transaction.
 * Therefore, in some cases, it may be possible for an annotated method to actually fix a validation problem,
 * by modifying a field or even deleting the object.
 *
 * <p>
 * However, by default Permazen invokes {@link OnValidate &#64;OnValidate} methods after the other validation checks
 * (i.e., JSR 303 and uniqueness constraints). This makes it easier to write {@link OnValidate &#64;OnValidate} methods
 * because one can assume those checks have already passed, but it also means it is too late fix any violations
 * of those checks.
 *
 * <p>
 * To have an {@link OnValidate &#64;OnValidate} method inovked prior to the other validation checks instead of
 * after them, set {@link #early}{@code  = true}. When doing this, be careful not to trigger infinite loops, as
 * {@link OnChange &#64;OnChange} notifications and {@link OnDelete &#64;OnDelete} notifications will still occur.
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring-framework/reference/core/beans/classpath-scanning.html#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 *
 * @see PermazenTransaction#validate
 * @see PermazenObject#revalidate
 * @see ValidationMode
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
public @interface OnValidate {

    /**
     * Specify the validation group(s) for which the annotated method should be invoked.
     *
     * @return validation group(s) to use for validation; if empty, {@link jakarta.validation.groups.Default} is assumed
     */
    Class<?>[] groups() default {};

    /**
     * Specify when to invoke the annotated method relative to JSR 303 and uniqueness constraint validation.
     *
     * <p>
     * If true, the annotated method will be invoked prior to JSR 303 and uniqueness constraint validation;
     * if false, afterward.
     *
     * @return true to invoke the annotated method prior other validation checks
     */
    boolean early() default false;
}
