
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Property;
import com.vaadin.data.util.AbstractProperty;

/**
 * Simple read-only {@link Property} implementation backed by a Java object.
 *
 * <p>
 * The {@link Property}s is defined using a {@link PropertyDef} along with a {@link PropertyExtractor}
 * that is capable of reading the property's value from the underlying Java object.
 * </p>
 *
 * @param <T> the type of the underlying Java object
 * @param <V> the type of the property
 * @see SimpleContainer
 * @see SimpleItem
 */
@SuppressWarnings("serial")
public class SimpleProperty<T, V> extends AbstractProperty<V> implements BackedProperty<T, V> {

    private final T object;
    private final PropertyDef<V> propertyDef;
    private final PropertyExtractor<? super T> propertyExtractor;

    /**
     * Constructor.
     *
     * @param object underlying Java object
     * @param propertyDef property definition
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    public SimpleProperty(T object, PropertyDef<V> propertyDef, PropertyExtractor<? super T> propertyExtractor) {
        if (object == null)
            throw new IllegalArgumentException("null object");
        if (propertyDef == null)
            throw new IllegalArgumentException("null propertyDef");
        if (propertyExtractor == null)
            throw new IllegalArgumentException("null propertyExtractor");
        this.object = object;
        this.propertyDef = propertyDef;
        this.propertyExtractor = propertyExtractor;
        this.setReadOnly(true);
    }

    @Override
    public T getObject() {
        return this.object;
    }

    @Override
    public Class<V> getType() {
        return this.propertyDef.getType();
    }

    @Override
    public V getValue() {
        return this.propertyExtractor.getPropertyValue(this.object, this.propertyDef);
    }

    /**
     * @throws Property.ReadOnlyException always
     */
    @Override
    public void setValue(V value) {
        throw new Property.ReadOnlyException();
    }

    /**
     * @throws UnsupportedOperationException if {@code readOnly} is true
     */
    @Override
    public void setReadOnly(boolean readOnly) {
        if (!readOnly)
            throw new UnsupportedOperationException();
        super.setReadOnly(readOnly);
    }
}

