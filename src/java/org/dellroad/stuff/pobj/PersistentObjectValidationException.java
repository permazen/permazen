
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.Set;

import javax.validation.ConstraintViolation;

/**
 * Runtime exception thrown during {@link PersistentObject} operations.
 */
@SuppressWarnings("serial")
public class PersistentObjectValidationException extends PersistentObjectException {

    private final Set<ConstraintViolation<?>> violations;

    public PersistentObjectValidationException(Set<ConstraintViolation<?>> violations) {
        this.violations = violations;
    }

    /**
     * Get the set of constraint violations.
     */
    public Set<ConstraintViolation<?>> getViolations() {
        return this.violations;
    }
}

