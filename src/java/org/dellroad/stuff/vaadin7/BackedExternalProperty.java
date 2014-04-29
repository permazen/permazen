
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

/**
 * {@link ExternalProperty} implementation that relies on some underlying Java object,
 * a {@link PropertyDef}, and and {@link PropertyExtractor}.
 *
 * @param <T> the type of the underlying Java object
 * @param <V> the type of the property
 * @see ExternalPropertyRegistry
 */
@SuppressWarnings("serial")
public class BackedExternalProperty<T, V> extends ExternalProperty<V> implements BackedProperty<T, V> {

    protected final T object;
    protected final PropertyDef<V> propertyDef;
    protected final PropertyExtractor<? super T> propertyExtractor;

    /**
     * Constructor. No initial value for the property will be set.
     *
     * @param registry registry for listener registrations
     * @param object underlying external Java object
     * @param propertyDef property definition
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    public BackedExternalProperty(ExternalPropertyRegistry registry, T object,
      PropertyDef<V> propertyDef, PropertyExtractor<? super T> propertyExtractor) {
        this(registry, object, propertyDef, propertyExtractor, null, false);
    }

    /**
     * Constructor. The property's value will be initially set to {@code initialValue}.
     *
     * @param registry registry for listener registrations
     * @param object underlying external Java object
     * @param initialValue initial property value
     * @param propertyDef property definition
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter other than {@code initialValue} is null
     */
    public BackedExternalProperty(ExternalPropertyRegistry registry, T object,
      PropertyDef<V> propertyDef, PropertyExtractor<? super T> propertyExtractor, V initialValue) {
        this(registry, object, propertyDef, propertyExtractor, initialValue, true);
    }

    BackedExternalProperty(ExternalPropertyRegistry registry, T object,
      PropertyDef<V> propertyDef, PropertyExtractor<? super T> propertyExtractor, V initialValue, boolean valid) {
        super(registry, initialValue, valid);
        if (object == null)
            throw new IllegalArgumentException("null object");
        if (propertyDef == null)
            throw new IllegalArgumentException("null propertyDef");
        if (propertyExtractor == null)
            throw new IllegalArgumentException("null propertyExtractor");
        this.object = object;
        this.propertyDef = propertyDef;
        this.propertyExtractor = propertyExtractor;
    }

    @Override
    public T getObject() {
        return this.object;
    }

    /**
     * Get the external identity of this property. Instances with the same external identity (where "same" means
     * {@link Object#equals equals()}) are assumed to depend on the same external information.
     *
     * <p>
     * The implementation in {@link BackedExternalProperty} returns the configured underlying Java object.
     * </p>
     *
     * @see ExternalPropertyRegistry#notifyValueChanged
     */
    @Override
    public Object getExternalIdentity() {
        return this.object;
    }

    @Override
    public Class<V> getType() {
        return this.propertyDef.getType();
    }

    /**
     * Calculate this instance's value.
     *
     * <p>
     * The implementation in {@link BackedExternalProperty} uses the configured {@link PropertyExtractor} to
     * extract the property value from the configured underlying Java object. Subclasses may need to override
     * to create transactions and/or acquire the appropriate external locks, etc.
     * </p>
     */
    @Override
    protected V calculateValue() {
        return this.propertyExtractor.getPropertyValue(this.object, this.propertyDef);
    }
}

