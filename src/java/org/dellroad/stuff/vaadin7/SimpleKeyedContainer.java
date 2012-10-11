
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.Collection;
import java.util.HashMap;

/**
 * An {@link AbstractSimpleContainer} where the item IDs are generated from the items themselves
 * by the subclass-provided method {@link #getKeyFor}.
 *
 * <p>
 * Restriction: instances can never contain two objects whose keys are equal (in the sense of {@link Object#equals}).
 *
 * @param <I> the item ID type
 * @param <T> the type of the Java objects that back each {@link com.vaadin.data.Item} in the container
 * @see AbstractSimpleContainer
 */
@SuppressWarnings("serial")
public abstract class SimpleKeyedContainer<I, T> extends AbstractSimpleContainer<I, T> {

    private HashMap<I, T> objectMap = new HashMap<I, T>(0);

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
    public SimpleKeyedContainer(PropertyExtractor<? super T> propertyExtractor) {
        super(propertyExtractor);
    }

    /**
     * Primary constructor.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects
     * @param propertyDefs container property definitions
     * @throws IllegalArgumentException if either parameter is null
     */
    public SimpleKeyedContainer(PropertyExtractor<? super T> propertyExtractor, Collection<? extends PropertyDef<?>> propertyDefs) {
        super(propertyExtractor, propertyDefs);
    }

    @Override
    public T getJavaObject(Object itemId) {
        if (itemId == null)
            return null;
        return this.objectMap.get(itemId);
    }

    /**
     * Get the container item ID corresponding to the given underlying Java object which is wrapped by this container.
     * Objects are tested for equality using {@link Object#equals Object.equals()}.
     *
     * <p>
     * This method uses an internal hash map for efficiency, and assumes that two underlying container objects that
     * are {@linkplain Object#equals equal} will have the same {@linkplain #getKeyFor key}.
     *
     * @param obj underlying container object
     * @return item ID corresponding to {@code object}, or null if {@code object} is not found in this container
     * @throws IllegalArgumentException if {@code object} is null
     * @see #getItemIdForSame
     */
    public I getItemIdFor(T obj) {
        if (obj == null)
            throw new IllegalArgumentException("null object");
        I key = this.getKeyFor(obj);
        if (key == null)
            throw new IllegalArgumentException("null key returned by getKeyFor() for object " + obj);
        T candidate = this.objectMap.get(key);
        return obj.equals(candidate) ? key : null;
    }

    /**
     * Get the container item ID corresponding to the given underlying Java object which is wrapped by this container.
     * Objects are tested for equality using object equality, not {@link Object#equals Object.equals()}.
     *
     * <p>
     * This method uses an internal hash map for efficiency.
     *
     * @param obj underlying container object
     * @return item ID corresponding to {@code object}, or null if {@code object} is not found in this container
     * @throws IllegalArgumentException if {@code object} is null
     * @see #getItemIdFor
     */
    public I getItemIdForSame(T obj) {
        if (obj == null)
            throw new IllegalArgumentException("null object");
        I key = this.getKeyFor(obj);
        if (key == null)
            throw new RuntimeException("null key returned by getKeyFor() for object " + obj);
        T candidate = this.objectMap.get(key);
        return obj == candidate ? key : null;
    }

    @Override
    protected void resetItemIds() {
        this.objectMap = new HashMap<I, T>();
    }

    @Override
    protected final I generateItemId(T obj) {
        I key = this.getKeyFor(obj);
        if (key == null)
            throw new RuntimeException("null key returned by getKeyFor() for object " + obj);
        T previous = this.objectMap.put(key, obj);
        if (previous != null)
            throw new RuntimeException("same key " + key + " used by two different objects " + previous + " and " + obj);
        return key;
    }

    /**
     * Get the key to be used as item ID for the given object.
     * All objects in the container must have unique keys.
     * This method must return the same key for the same object even if invoked multiple times.
     *
     * @param obj underlying container object
     * @return key for object
     */
    protected abstract I getKeyFor(T obj);
}

