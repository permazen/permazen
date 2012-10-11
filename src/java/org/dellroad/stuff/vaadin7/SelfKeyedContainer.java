
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.Collection;

/**
 * A {@link SimpleKeyedContainer} where the item IDs are the underlying container objects themselves.
 *
 * <p>
 * Restriction: instances can't contain two objects that are {@link Object#equals equal()} to each other.
 *
 * @param <T> the type of the Java objects that back each {@link com.vaadin.data.Item} in the container
 */
@SuppressWarnings("serial")
public class SelfKeyedContainer<T> extends SimpleKeyedContainer<T, T> {

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setProperties setProperties()} is required
     * to define the properties of this container.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects
     * @throws IllegalArgumentException if {@code propertyExtractor} is null
     */
    public SelfKeyedContainer(PropertyExtractor<? super T> propertyExtractor) {
        super(propertyExtractor);
    }

    /**
     * Primary constructor.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects
     * @param propertyDefs container property definitions
     * @throws IllegalArgumentException if either parameter is null
     */
    public SelfKeyedContainer(PropertyExtractor<? super T> propertyExtractor, Collection<? extends PropertyDef<?>> propertyDefs) {
        super(propertyExtractor, propertyDefs);
    }

    /**
     * Get the key to be used as item ID for the given object.
     *
     * <p>
     * The implementation in {@link SelfKeyedContainer} always returns {@code obj}.
     *
     * @param obj underlying container object
     * @return key for object
     */
    @Override
    protected T getKeyFor(T obj) {
        return obj;
    }
}

