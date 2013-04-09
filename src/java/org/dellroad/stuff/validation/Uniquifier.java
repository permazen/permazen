
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

/**
 * Converts values into some other object that has the desired uniqueness.
 *
 * @param <T> the type of object being validated
 * @see Unique @Unique
 */
public interface Uniquifier<T> {

    /**
     * Get an object representing the unique value of the given instance.
     * In other words, if and only two values are "the same", this method should return object(s) that
     * match (according to {@link Object#equals equals()} and {@link Object#hashCode}).
     *
     * <p>
     * This method may return a value of {@code null} to indicate that {@code value} does not in fact
     * need to be unique.
     * </p>
     *
     * @param value the value to be uniquified
     * @return unique representative, or {@code null} to except {@code value} from the uniqueness requirement
     * @throws ClassCastException if {@code value} is not of a supported type
     */
    Object getUniqued(T value);
}

