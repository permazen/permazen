
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
 * Thrown when a validating {@link JTransaction} is committed and validation fails.
 */
@SuppressWarnings("serial")
public class ValidationException extends DatabaseException {

    private final JObject jobj;
    private final Set<ConstraintViolation<JObject>> violations;

    public ValidationException(JObject jobj, Set<ConstraintViolation<JObject>> violations, String message) {
        super(message);
        this.jobj = jobj;
        this.violations = violations;
    }

    /**
     * Get the object that failed to validate.
     */
    public JObject getObject() {
        return this.jobj;
    }

    /**
     * Get the validation errors.
     */
    public Set<ConstraintViolation<JObject>> getViolations() {
        return this.violations;
    }
}

