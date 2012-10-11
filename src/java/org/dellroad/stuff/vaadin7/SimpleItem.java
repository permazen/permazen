
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Item;
import com.vaadin.data.Property;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Simple read-only {@link Item} implementation backed by a Java object.
 *
 * <p>
 * Item {@link Property}s are defined by {@link PropertyDef}s and retrieved by a {@link PropertyExtractor}.
 *
 * @param <T> the type of the underlying Java object
 * @see SimpleContainer
 * @see SimpleProperty
 */
@SuppressWarnings("serial")
public class SimpleItem<T> implements Item {

    private final T object;
    private final Map<String, ? extends PropertyDef<?>> propertyMap;
    private final PropertyExtractor<? super T> propertyExtractor;

    /**
     * Constructor.
     *
     * @param object underlying Java object
     * @param propertyDefs property definitions
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    public SimpleItem(T object, Collection<? extends PropertyDef<?>> propertyDefs, PropertyExtractor<? super T> propertyExtractor) {
        this(object, SimpleItem.buildPropertyMap(propertyDefs), propertyExtractor);
    }

    /**
     * Constructor used when the mapping from property name to {@link PropertyDef} is already built.
     *
     * @param object underlying Java object
     * @param propertyMap mapping from property name to property definition
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    public SimpleItem(T object, Map<String, ? extends PropertyDef<?>> propertyMap, PropertyExtractor<? super T> propertyExtractor) {
        if (object == null)
            throw new IllegalArgumentException("null object");
        if (propertyMap == null)
            throw new IllegalArgumentException("null propertyMap");
        if (propertyExtractor == null)
            throw new IllegalArgumentException("null propertyExtractor");
        this.object = object;
        this.propertyMap = propertyMap;
        this.propertyExtractor = propertyExtractor;
    }

    /**
     * Retrieve the underlying Java object.
     *
     * @return underlying Java object, never null
     */
    public T getObject() {
        return this.object;
    }

    @Override
    public SimpleProperty<T, ?> getItemProperty(Object id) {
        PropertyDef<?> propertyDef = this.propertyMap.get(id);
        if (propertyDef == null)
            return null;
        return this.createSimpleProperty(propertyDef);
    }

    @Override
    public Set<String> getItemPropertyIds() {
        return Collections.unmodifiableSet(this.propertyMap.keySet());
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public boolean addItemProperty(Object id, Property property) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public boolean removeItemProperty(Object id) {
        throw new UnsupportedOperationException();
    }

    // This method exists only to allow the generic parameter <V> to be bound
    private <V> SimpleProperty<T, V> createSimpleProperty(PropertyDef<V> propertyDef) {
        return new SimpleProperty<T, V>(this.object, propertyDef, this.propertyExtractor);
    }

    private static Map<String, PropertyDef<?>> buildPropertyMap(Collection<? extends PropertyDef<?>> propertyDefs) {
        HashMap<String, PropertyDef<?>> map = new HashMap<String, PropertyDef<?>>(propertyDefs.size());
        for (PropertyDef<?> propertyDef : propertyDefs)
            map.put(propertyDef.getName(), propertyDef);
        return map;
    }
}

