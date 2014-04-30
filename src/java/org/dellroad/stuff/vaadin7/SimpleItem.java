
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple read-only {@link com.vaadin.data.Item} implementation backed by a Java object.
 *
 * <p>
 * Item {@link com.vaadin.data.Property}s are defined by {@link PropertyDef}s and retrieved by a {@link PropertyExtractor}.
 * </p>
 *
 * <p>
 * Although the item properties are read-only from "above", they can be made mutable from "below" by modifying the
 * backing object and invoking {@link #fireValueChange fireValueChange()}.
 * </p>
 *
 * @param <T> the type of the underlying Java object
 * @see AbstractQueryContainer
 * @see AbstractSimpleContainer
 */
@SuppressWarnings("serial")
public class SimpleItem<T> implements BackedItem<T> {

    protected final T object;
    protected final PropertyExtractor<? super T> propertyExtractor;

    private final Object[] propertyList;                    // elements are either PropertyDef's or Property's

    /**
     * Constructor.
     *
     * @param object underlying Java object
     * @param propertyDefs property definitions
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    public SimpleItem(T object, Collection<? extends PropertyDef<?>> propertyDefs, PropertyExtractor<? super T> propertyExtractor) {
        if (object == null)
            throw new IllegalArgumentException("null object");
        if (propertyExtractor == null)
            throw new IllegalArgumentException("null propertyExtractor");
        if (propertyDefs == null)
            throw new IllegalArgumentException("null propertyDefs");
        this.object = object;
        this.propertyExtractor = propertyExtractor;
        this.propertyList = propertyDefs.toArray();
    }

    @Override
    public T getObject() {
        return this.object;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Property<?> getItemProperty(Object id) {
        for (int i = 0; i < this.propertyList.length; i++) {
            final Object value = this.propertyList[i];
            if (value instanceof PropertyDef) {
                final PropertyDef<?> propertyDef = (PropertyDef<?>)value;
                if (!propertyDef.getName().equals(id))
                    continue;
                final Property<?> property = this.createProperty(propertyDef);
                this.propertyList[i] = property;
                return property;
            } else if (value instanceof Property) {
                final Property<?> property = (Property<?>)value;
                if (!property.getPropertyDef().getName().equals(id))
                    continue;
                return property;
            } else
                throw new RuntimeException("internal error: found " + value);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getItemPropertyIds() {
        final HashSet<String> names = new HashSet<String>(this.propertyList.length);
        for (int i = 0; i < this.propertyList.length; i++) {
            final Object value = this.propertyList[i];
            if (value instanceof PropertyDef) {
                final PropertyDef<?> propertyDef = (PropertyDef<?>)value;
                names.add(propertyDef.getName());
            } else if (value instanceof Property) {
                final Property<?> property = (Property<?>)value;
                names.add(property.getPropertyDef().getName());
            } else
                throw new RuntimeException("internal error: found " + value);
        }
        return names;
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    @SuppressWarnings("rawtypes")
    public boolean addItemProperty(Object id, com.vaadin.data.Property/*<?>*/ property) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public boolean removeItemProperty(Object id) {
        throw new UnsupportedOperationException();
    }

    /**
     * Issue {@link com.vaadin.data.Property.ValueChangeEvent}s to all
     * {@link com.vaadin.data.Property.ValueChangeListener}s registered on the specified property of this item.
     *
     * <p>
     * Does nothing if {@code propertyName} is not one of this item's properties.
     * </p>
     *
     * @param propertyName property ID
     */
    @SuppressWarnings("unchecked")
    public void fireValueChange(String propertyName) {
        for (int i = 0; i < this.propertyList.length; i++) {
            final Object value = this.propertyList[i];
            if (value instanceof Property) {
                final Property<?> property = (Property<?>)value;
                if (property.getPropertyDef().getName().equals(propertyName)) {
                    property.fireValueChange();
                    return;
                }
            }
        }
    }

    /**
     * Issue {@link com.vaadin.data.Property.ValueChangeEvent}s to all
     * {@link com.vaadin.data.Property.ValueChangeListener}s registered on any property of this item.
     */
    @SuppressWarnings("unchecked")
    public void fireValueChange() {
        for (int i = 0; i < this.propertyList.length; i++) {
            final Object value = this.propertyList[i];
            if (value instanceof Property) {
                final Property<?> property = (Property<?>)value;
                property.fireValueChange();
            }
        }
    }

    /**
     * Create a {@link Property} to be used for the given property definition.
     * This method will be invoked at most once for any property; the returned value is cached.
     *
     * <p>
     * The implementation in {@link SimpleItem} returns {@code new Property<V>(propertyDef)}.
     * </p>
     *
     * @param propertyDef property definition
     * @throws IllegalArgumentException if {@code propertyDef} is null
     */
    protected <V> Property<V> createProperty(PropertyDef<V> propertyDef) {
        return new Property<V>(propertyDef);
    }

// Property

    /**
     * {@link Property} implementation used by {@link SimpleItem}.
     */
    public class Property<V> extends ReadOnlyProperty<V> implements BackedProperty<T, V> {

        private final PropertyDef<V> propertyDef;

        public Property(PropertyDef<V> propertyDef) {
            if (propertyDef == null)
                throw new IllegalArgumentException("null propertyDef");
            this.propertyDef = propertyDef;
        }

        public PropertyDef<V> getPropertyDef() {
            return this.propertyDef;
        }

        @Override
        public T getObject() {
            return SimpleItem.this.object;
        }

        @Override
        public Class<V> getType() {
            return this.propertyDef.getType();
        }

        @Override
        public V getValue() {
            return SimpleItem.this.propertyExtractor.getPropertyValue(SimpleItem.this.object, this.propertyDef);
        }
    }
}

