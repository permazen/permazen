
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
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

    /**
     * Constructor.
     *
     * @param violations set of violations
     * @throws IllegalArgumentException if {@code violations} is null
     */
    public PersistentObjectValidationException(Set<ConstraintViolation<?>> violations) {
        super(PersistentObjectValidationException.generateMessage(violations));
        this.violations = violations;
    }

    /**
     * Get the set of constraint violations.
     */
    public Set<ConstraintViolation<?>> getViolations() {
        return this.violations;
    }

    private static String generateMessage(Set<ConstraintViolation<?>> violations) {
        if (violations == null)
            throw new IllegalArgumentException("null violations");
        StringBuilder buf = new StringBuilder("object failed to validate with " + violations.size() + " violation(s):\n");
        boolean first = true;
        for (ConstraintViolation<?> violation : violations) {
            buf.append("    [")
              .append(violation.getPropertyPath())
              .append("]: ")
              .append(violation.getMessage())
              .append('\n');
        }
        return buf.toString();
    }
}

