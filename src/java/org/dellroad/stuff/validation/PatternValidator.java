
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import java.util.Collection;
import java.util.regex.PatternSyntaxException;

import javax.validation.ConstraintDeclarationException;
import javax.validation.ConstraintValidatorContext;

/**
 * Validator for the @{@link Pattern} constraint.
 *
 * @see Pattern
 */
public class PatternValidator extends AbstractValidator<Pattern, Object> {

    private java.util.regex.Pattern pattern;

    @Override
    public void initialize(Pattern pattern) {
        super.initialize(pattern);

        // Get flags
        int flags = 0;
        for (javax.validation.constraints.Pattern.Flag flag : this.annotation.flags())
            flags |= flag.getValue();

        // Compile regular expression
        try {
            this.pattern = java.util.regex.Pattern.compile(this.annotation.regexp(), flags);
        } catch (PatternSyntaxException e) {
            throw new ConstraintDeclarationException("Invalid regular expression `" + annotation.regexp() + "': " + e.getMessage());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean isValid(Object value, ConstraintValidatorContext context) {

        // Handle null
        if (value == null)
            return true;

        // Handle collection
        if (value instanceof Collection)
            return this.isCollectionValid((Collection<?>)value, context);

        // Check pattern
        return this.pattern.matcher(value.toString()).matches();
    }
}

