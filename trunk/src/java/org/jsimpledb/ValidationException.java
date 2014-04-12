
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.Set;

import javax.validation.ConstraintViolation;

import org.jsimpledb.core.DatabaseException;

/**
 * Thrown when {@link JTransaction#validate} (or {@link JTransaction#commit}) fails due to one or more validation errors.
 *
 * @see JTransaction#validate
 */
@SuppressWarnings("serial")
public class ValidationException extends DatabaseException {

    private final JObject jobj;
    private final Set<ConstraintViolation<JObject>> violations;

    /**
     * Constructor.
     *
     * @param jobj the object that failed validation
     * @param violations JSR 303 validation errors, if any, otherwise null
     * @param message exception message
     */
    public ValidationException(JObject jobj, Set<ConstraintViolation<JObject>> violations, String message) {
        super(message);
        this.jobj = jobj;
        this.violations = violations;
    }

    /**
     * Convenience constructor for use when JSR 303 validation is not involved.
     *
     * @param jobj the object that failed validation
     * @param message exception message
     */
    public ValidationException(JObject jobj, String message) {
        this(jobj, null, message);
    }

    /**
     * Get the object that failed to validate.
     */
    public JObject getObject() {
        return this.jobj;
    }

    /**
     * Get the validation errors in case validation failed due to a failed JSR 303 validation check.
     *
     * @return set of JSR 303 validation errors, or null if validation did not fail due to JSR 303 validation
     */
    public Set<ConstraintViolation<JObject>> getViolations() {
        return this.violations;
    }
}

