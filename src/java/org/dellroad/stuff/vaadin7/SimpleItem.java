
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Property;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Simple read-only {@link com.vaadin.data.Item} implementation backed by a Java object.
 *
 * <p>
 * Item {@link Property}s are defined by {@link PropertyDef}s and retrieved by a {@link PropertyExtractor}.
 *
 * @param <T> the type of the underlying Java object
 * @see SimpleContainer
 * @see SimpleProperty
 */
@SuppressWarnings("serial")
public class SimpleItem<T> implements BackedItem<T> {

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

    @Override
    public T getObject() {
        return this.object;
    }

    @Override
    public Property<?> getItemProperty(Object id) {
        final PropertyDef<?> propertyDef = this.propertyMap.get(id);
        if (propertyDef == null)
            return null;
        return this.createProperty(this.object, propertyDef, this.propertyExtractor);
    }

    @Override
    public Set<String> getItemPropertyIds() {
        return Collections.unmodifiableSet(this.propertyMap.keySet());
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    @SuppressWarnings("rawtypes")
    public boolean addItemProperty(Object id, Property/*<?>*/ property) {
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
     * Create a {@link Property} for the given property definition.
     *
     * <p>
     * The implementation in {@link SimpleItem} returns
     * {@code new SimpleProperty<T, V>(object, propertyDef, propertyExtractor)}.
     * </p>
     *
     * @param object underlying Java object
     * @param propertyDef property definition
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    protected <V> Property<V> createProperty(T object,
      PropertyDef<V> propertyDef, PropertyExtractor<? super T> propertyExtractor) {
        return new SimpleProperty<T, V>(object, propertyDef, propertyExtractor);
    }

    private static Map<String, PropertyDef<?>> buildPropertyMap(Collection<? extends PropertyDef<?>> propertyDefs) {
        HashMap<String, PropertyDef<?>> map = new HashMap<String, PropertyDef<?>>(propertyDefs.size());
        for (PropertyDef<?> propertyDef : propertyDefs)
            map.put(propertyDef.getName(), propertyDef);
        return map;
    }
}

