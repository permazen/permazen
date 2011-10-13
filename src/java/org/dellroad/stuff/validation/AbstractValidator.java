
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import java.lang.annotation.Annotation;
import java.util.Collection;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * Support superclass for validators.
 */
public abstract class AbstractValidator<C extends Annotation, T> implements ConstraintValidator<C, T> {

    /**
     * The constraint being checked by this instance.
     */
    protected C annotation;

    @Override
    public void initialize(@SuppressWarnings("hiding") C annotation) {
        this.annotation = annotation;
    }

    /**
     * Convenience method to add a constraint violation described by {@code message} and disable the default violation.
     */
    protected void setViolation(ConstraintValidatorContext context, String message) {
        context.disableDefaultConstraintViolation();
        context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
    }

    /**
     * Apply constraint to all values in a collection. This is a convenience method for validators
     * that want to work with both simple properties and collection properties.
     */
    protected boolean isCollectionValid(Collection<? extends T> collection, ConstraintValidatorContext context) {
        boolean result = true;
        for (T value : collection) {
            if (!this.isValid(value, context))
                result = false;
        }
        return result;
    }
}

