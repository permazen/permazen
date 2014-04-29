
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.Collection;
import java.util.Map;

/**
 * Extension of {@link SimpleItem} that creates {@link ExternalProperty}s registered with a configured
 * {@link ExternalPropertyRegistry}.
 *
 * @param <T> the type of the underlying Java object
 */
@SuppressWarnings("serial")
public class BackedExternalItem<T> extends SimpleItem<T> {

    protected final ExternalPropertyRegistry registry;

    /**
     * Constructor.
     *
     * @param registry registry for property listener registrations
     * @param object underlying Java object
     * @param propertyDefs property definitions
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    public BackedExternalItem(ExternalPropertyRegistry registry, T object,
      Collection<? extends PropertyDef<?>> propertyDefs, PropertyExtractor<? super T> propertyExtractor) {
        super(object, propertyDefs, propertyExtractor);
        if (registry == null)
            throw new IllegalArgumentException("null registry");
        this.registry = registry;
    }

    /**
     * Constructor used when the mapping from property name to {@link PropertyDef} is already built.
     *
     * @param registry registry for property listener registrations
     * @param object underlying Java object
     * @param propertyMap mapping from property name to property definition
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    public BackedExternalItem(ExternalPropertyRegistry registry, T object,
      Map<String, ? extends PropertyDef<?>> propertyMap, PropertyExtractor<? super T> propertyExtractor) {
        super(object, propertyMap, propertyExtractor);
        if (registry == null)
            throw new IllegalArgumentException("null registry");
        this.registry = registry;
    }

    /**
     * Create an {@link ExternalProperty} for the given property definition.
     *
     * <p>
     * The implementation in {@link BackedExternalItem} returns
     * {@code new BackedExternalProperty<T, V>(this.registry, object, propertyDef, propertyExtractor)}.
     * </p>
     *
     * @param object underlying Java object
     * @param propertyDef property definition
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    @Override
    protected <V> ExternalProperty<V> createProperty(T object,
      PropertyDef<V> propertyDef, PropertyExtractor<? super T> propertyExtractor) {
        return new BackedExternalProperty<T, V>(this.registry, object, propertyDef, propertyExtractor);
    }
}

