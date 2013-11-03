
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

/**
 * {@link PropertyExtractor} that also sorts properties, given two instances of the target Java type.
 *
 * @param <T> target object type for extraction
 */
public interface SortingPropertyExtractor<T> extends PropertyExtractor<T> {

    /**
     * Determine if the given property can be sorted by this instance.
     * If this method returns false, then the property's sorting resorts to the default behavior,
     * i.e., depending on whether the property value implements {@link java.util.Comparable}.
     *
     * @param propertyDef definition of property
     * @return true if the property defined by {@code propertyDef} can be sorted by this instance
     * @throws NullPointerException if {@code propertyDef} is null
     */
    boolean canSort(PropertyDef<?> propertyDef);

    /**
     * Sort two values based on the given property.
     *
     * @param obj1 first Java object
     * @param obj2 second Java object
     * @param propertyDef definition of property to sort on
     * @return result of comparing {@code obj1}'s property value to {@code obj2}'s property value
     * @throws NullPointerException if any parameter is null
     * @throws UnsupportedOperationException if the property defined by {@code propertyDef} cannot be sorted by this instance
     */
    int sort(PropertyDef<?> propertyDef, T obj1, T obj2);
}

