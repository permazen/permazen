
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import javax.validation.ConstraintValidatorContext;

/**
 * Validator for the @{@link Sorted} constraint.
 *
 * @see Sorted
 */
public class SortedValidator extends AbstractValidator<Sorted, Object> {

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean isValid(Object value, ConstraintValidatorContext context) {

        // Ignore null values
        if (value == null)
            return true;

        // Value must be an array, collection or map
        Iterable<?> iterable;
        if (value instanceof Object[])
            iterable = Arrays.asList((Object[])value);
        else if (value instanceof Collection)
            iterable = (Collection<?>)value;
        else if (value instanceof Map)
            iterable = ((Map<?, ?>)value).keySet();
        else {
            this.setViolation(context, "@Sorted constraint only applies to non-primitive arrays, collections and maps");
            return false;
        }

        // Get comparator
        Comparator comparator;
        if (this.annotation.comparator() == Comparator.class) {
            comparator = new Comparator() {
                @Override
                public int compare(Object x, Object y) {
                    return ((Comparable)x).compareTo(y);
                }
            };
        } else {
            try {
                comparator = this.annotation.comparator().newInstance();
            } catch (Exception e) {
                this.setViolation(context, "Cannot instantiate comparator for @Sorted constraint: " + e);
                return false;
            }
        }

        // Check sorted-ness
        Object prev = null;
        int index = -1;
        for (Object next : iterable) {
            index++;
            if (prev == null) {
                prev = next;
                continue;
            }
            if (next == null)
                continue;
            int diff;
            try {
                diff = comparator.compare(prev, next);
            } catch (ClassCastException e) {
                this.setViolation(context, "@Sorted constraint only applies to Comparable elements: " + e);
                return false;
            }
            if (diff > 0 || (this.annotation.strict() && diff == 0)) {
                this.setViolation(context, "elements are not properly sorted (mis-ordered at index " + index + ")");
                return false;
            }
        }

        // Done
        return true;
    }
}

