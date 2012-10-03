
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

/**
 * Classes that can extract {@link com.vaadin.data.Property} values from Java objects.
 *
 * @param <T> target object type for extraction
 */
public interface PropertyExtractor<T> {

    /**
     * Read the value of the property defined by {@code propertyDef} from the given object.
     *
     * @param obj Java object
     * @param propertyDef definition of which property to read
     * @throws NullPointerException if either parameter is null
     */
    <V> V getPropertyValue(T obj, PropertyDef<V> propertyDef);
}

