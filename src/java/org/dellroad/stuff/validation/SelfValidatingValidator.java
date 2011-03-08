
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * Implements the validation logic for the @{@link SelfValidates} annotation.
 *
 * @see SelfValidates
 */
public class SelfValidatingValidator implements ConstraintValidator<SelfValidates, SelfValidating> {

    @Override
    public void initialize(SelfValidates annotation) {
    }

    @Override
    public boolean isValid(SelfValidating value, ConstraintValidatorContext context) {
        try {
            value.checkValid(context);
            return true;
        } catch (SelfValidationException e) {
            String message = e.getMessage();
            if (message != null) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
            }
            return false;
        }
    }
}

