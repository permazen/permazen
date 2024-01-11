
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.DatabaseException;

import jakarta.validation.ConstraintViolation;

import java.util.Set;

/**
 * Thrown when {@link PermazenTransaction#validate} (or {@link PermazenTransaction#commit}) fails due to one or more validation errors.
 *
 * @see PermazenTransaction#validate
 */
@SuppressWarnings("serial")
public class ValidationException extends DatabaseException {

    private final PermazenObject pobj;
    private final Set<ConstraintViolation<PermazenObject>> violations;

    /**
     * Constructor.
     *
     * @param pobj the object that failed validation
     * @param violations JSR 303 validation errors, if any, otherwise null
     * @param message exception message
     */
    public ValidationException(PermazenObject pobj, Set<ConstraintViolation<PermazenObject>> violations, String message) {
        super(message);
        this.pobj = pobj;
        this.violations = violations;
    }

    /**
     * Convenience constructor for use when JSR 303 validation is not involved.
     *
     * @param pobj the object that failed validation
     * @param message exception message
     */
    public ValidationException(PermazenObject pobj, String message) {
        this(pobj, message, null);
    }

    /**
     * Convenience constructor for use when JSR 303 validation is not involved.
     *
     * @param pobj the object that failed validation
     * @param message exception message
     * @param cause underlying cause, or null for none
     */
    @SuppressWarnings("this-escape")
    public ValidationException(PermazenObject pobj, String message, Throwable cause) {
        this(pobj, null, message);
        if (cause != null)
            this.initCause(cause);
    }

    /**
     * Get the object that failed to validate.
     *
     * @return the invalid object
     */
    public PermazenObject getObject() {
        return this.pobj;
    }

    /**
     * Get the validation errors in case validation failed due to a failed JSR 303 validation check.
     *
     * @return set of JSR 303 validation errors, or null if validation did not fail due to JSR 303 validation
     */
    public Set<ConstraintViolation<PermazenObject>> getViolations() {
        return this.violations;
    }
}
