
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

/**
 * Provides additional context for {@link javax.validation.ConstraintValidator} implementations.
 *
 * <p>
 * {@link ValidationContext} gives {@link javax.validation.ConstraintValidator} implementations access to the root object
 * being validated. This breaks the usual principle of locality for validation (i.e., that validation of a specific bean
 * proceeds unaware of that bean's parents) but it can make custom validators more convenient to implement.
 * Subclasses are encouraged to provide additional application-specific information.
 *
 * <p>
 * Validation must be performed via {@link #validate(Validator, ValidationContext) validate()} for this class to work.
 */
public class ValidationContext<T> {

    static final ThreadLocal<ValidationContext> CURRENT = new ThreadLocal<ValidationContext>();

    private final T root;

    /**
     * Construct a new validation context configured to validate the given root object.
     *
     * @param root root object to be validated
     * @throws IllegalArgumentException if {@code root} is null
     */
    public ValidationContext(T root) {
        if (root == null)
            throw new IllegalArgumentException("null root");
        this.root = root;
    }

    /**
     * Get the root object associated with this instance.
     */
    public final T getRoot() {
        return this.root;
    }

    /**
     * Convenience method. Equivalent to:
     *  <blockquote>
     *  <code>{@link #validate ValidationContext.validate}(Validation.buildDefaultValidatorFactory().getValidator(), context)</code>
     *  <blockquote>
     *
     * @throws IllegalStateException if this method is invoked re-entrantly
     * @throws IllegalArgumentException if {@code context} is null
     */
    public static <T> Set<ConstraintViolation<T>> validate(ValidationContext<T> context) {
        return ValidationContext.validate(Validation.buildDefaultValidatorFactory().getValidator(), context);
    }

    /**
     * Validate the given context's root object using the given {@link Validator}, making the context
     * available to the current thread during the validation process via {@link #getCurrentContext}.
     *
     * @throws IllegalStateException if this method is invoked re-entrantly
     * @throws IllegalArgumentException if {@code context} is null
     */
    public static <T> Set<ConstraintViolation<T>> validate(Validator validator, ValidationContext<T> context) {

        // Sanity check
        if (ValidationContext.CURRENT.get() != null)
            throw new IllegalStateException("re-entrant invocation is not allowed");
        if (context == null)
            throw new IllegalStateException("null context");

        // Set context, then validate
        ValidationContext.CURRENT.set(context);
        try {
            return validator.validate(context.getRoot());
        } finally {
            ValidationContext.CURRENT.remove();
        }
    }

    /**
     * Get the {@link ValidationContext} associated with the current thread, cast to the desired type.
     * This method is only valid during invocations of {@link #validate(Validator, ValidationContext) validate()}.
     *
     * @param type required type
     * @return current {@link ValidationContext}
     * @throws IllegalStateException if {@link #validate(Validator, ValidationContext) validate()} is not currently executing
     * @throws ClassCastException if the current {@link ValidationContext} is not of type {@code type}
     */
    public static <T extends ValidationContext<?>> T getCurrentContext(Class<T> type) {
        ValidationContext<?> context = ValidationContext.CURRENT.get();
        if (context == null)
            throw new IllegalStateException("current thread is not executing validate()");
        return type.cast(context);
    }

    /**
     * Convenience method to get the root object being validated by the current thread.
     * This method is only valid during invocations of {@link #validate(Validator, ValidationContext) validate()}.
     * Subclasses may want to override to narrow the return type.
     *
     * @return current validation root object
     * @throws IllegalStateException if {@link #validate(Validator, ValidationContext) validate()} is not currently executing
     */
    public static Object getCurrentRoot() {
        return ValidationContext.getCurrentContext(ValidationContext.class).getRoot();
    }
}

