
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
 * Validation must be performed via {@link #validate(Validator) validate()} for this class to work.
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
     * Validate this instance's root object. This is a convenience method, equivalent to:
     *  <blockquote>
     *  <code>{@link #validate(Validator) validate}(Validation.buildDefaultValidatorFactory().getValidator())</code>
     *  <blockquote>
     *
     * @throws IllegalStateException if this method is invoked re-entrantly
     */
    public Set<ConstraintViolation<T>> validate() {
        return this.validate(Validation.buildDefaultValidatorFactory().getValidator());
    }

    /**
     * Validate this instance's root object using the given {@link Validator}, making the context
     * available to the current thread during the validation process via {@link #getCurrentContext}.
     *
     * @throws IllegalStateException if this method is invoked re-entrantly
     */
    public Set<ConstraintViolation<T>> validate(Validator validator) {

        // Sanity check
        if (ValidationContext.CURRENT.get() != null)
            throw new IllegalStateException("re-entrant invocation is not allowed");

        // Set context, then validate
        ValidationContext.CURRENT.set(this);
        try {
            return validator.validate(this.root);
        } finally {
            ValidationContext.CURRENT.remove();
        }
    }

    /**
     * Get the {@link ValidationContext} associated with the current thread, cast to the desired type.
     * This method is only valid during invocations of {@link #validate(Validator) validate()}.
     *
     * @param type required type
     * @return current {@link ValidationContext}
     * @throws IllegalStateException if {@link #validate(Validator) validate()} is not currently executing
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
     * This method is only valid during invocations of {@link #validate(Validator) validate()}.
     * Subclasses may want to override to narrow the return type.
     *
     * @return current validation root object
     * @throws IllegalStateException if {@link #validate(Validator) validate()} is not currently executing
     */
    public static Object getCurrentRoot() {
        return ValidationContext.getCurrentContext(ValidationContext.class).getRoot();
    }
}

