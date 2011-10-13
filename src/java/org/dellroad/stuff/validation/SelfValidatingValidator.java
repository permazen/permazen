
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import javax.validation.ConstraintValidatorContext;

/**
 * Implements the validation logic for the {@link SelfValidates @SelfValidates} annotation.
 *
 * @see SelfValidates
 */
public class SelfValidatingValidator extends AbstractValidator<SelfValidates, SelfValidating> {

    @Override
    public boolean isValid(SelfValidating value, ConstraintValidatorContext context) {
        try {
            value.checkValid(context);
            return true;
        } catch (SelfValidationException e) {
            String message = e.getMessage();
            if (message != null)
                this.setViolation(context, message);
            return false;
        }
    }
}

