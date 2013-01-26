
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import java.util.Set;

import javax.validation.ConstraintViolation;

/**
 * Validation utility methods.
 */
public final class ValidationUtil {

    private ValidationUtil() {
    }

    /**
     * Describe the validation errors in a friendly format.
     *
     * @param violations validation violations
     * @return description of the validation errors
     * @throws NullPointerException if {@code violations} is null
     */
    public static String describe(Set<? extends ConstraintViolation<?>> violations) {
        if (violations.isEmpty())
            return "  (no violations)";
        StringBuilder buf = new StringBuilder(violations.size() * 32);
        for (ConstraintViolation<?> violation : violations) {
            String violationPath = violation.getPropertyPath().toString();
            buf.append("  ");
            if (violationPath.length() > 0)
                buf.append("[").append(violationPath).append("]: ");
            buf.append(violation.getMessage()).append('\n');
        }
        return buf.toString();
    }
}

