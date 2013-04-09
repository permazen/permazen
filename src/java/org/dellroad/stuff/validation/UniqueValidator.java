
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.validation.ConstraintValidatorContext;

/**
 * Validator for the {@link Unique @Unique} constraint.
 *
 * @see Unique
 */
public class UniqueValidator extends AbstractValidator<Unique, Object> {

    private String domain;
    private Uniquifier<Object> uniquifier;

    @Override
    @SuppressWarnings("unchecked")
    public void initialize(Unique annotation) {
        super.initialize(annotation);
        this.domain = annotation.domain();
        Class<? extends Uniquifier<?>> uniquifierClass = annotation.uniquifier();
        try {
            this.uniquifier = (Uniquifier<Object>)uniquifierClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("can't create an instance of " + uniquifierClass + ": " + e.getMessage());
        }
    }

    @Override
    public boolean isValid(final Object propertyValue, ConstraintValidatorContext context) {

        // Ignore null values
        if (propertyValue == null)
            return true;

        // Recurse on collections and maps
        if (propertyValue instanceof Collection)
            return this.isCollectionValid((Collection<?>)propertyValue, context);
        if (propertyValue instanceof Map)
            return this.isCollectionValid(((Map<?, ?>)propertyValue).values(), context);

        // If we are only validating a sub-tree then skip this check
        ValidationContext<?> validationContext;
        try {
            validationContext = ValidationContext.getCurrentContext();
        } catch (ClassCastException e) {
            return true;
        }

        // Uniquify value
        Object uniqueValue;
        try {
            uniqueValue = this.uniquifier.getUniqued(propertyValue);
        } catch (ClassCastException e) {
            throw new RuntimeException("uniquifier " + this.annotation.uniquifier()
              + " does not support uniquifying values of type " + propertyValue.getClass());
        }

        // Ignore null values
        if (uniqueValue == null)
            return true;

        // Get the domain
        final Set<Object> uniqueDomain = validationContext.getUniqueDomain(this.domain);

        // Verify value is unique XXX: this assumes isValid() is invoked only once per property
        if (!uniqueDomain.add(uniqueValue)) {
            this.setViolation(context, "non-unique value `" + uniqueValue + "' in uniqueness domain `" + this.domain + "'");
            return false;
        }

        // Done
        return true;
    }
}

