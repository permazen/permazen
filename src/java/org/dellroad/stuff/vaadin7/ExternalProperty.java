
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

/**
 * Support superclass for {@link com.vaadin.data.Property}s whose value depends on information that is external to a
 * {@link com.vaadin.server.VaadinSession}, yet who also wish to support notification of
 * {@link com.vaadin.data.Property.ValueChangeListeners}s within individual {@link com.vaadin.server.VaadinSession}s
 * when that external information changes. Instances are read-only from the Vaadin perspective, but mutable
 * from the "back-end" perspective.
 *
 * <p>
 * Instances cache their property values. When {@link #getValue} is invoked and no property value is cached,
 * {@linkplain #calculateValue} is invoked to (re)calculate it. Cached values remain valid until
 * {@link #invalidateCachedValue} is invoked.
 * </p>
 *
 * <p>
 * Instances reference an {@link ExternalPropertyRegistry}, which is not associated with any specific
 * {@link com.vaadin.server.VaadinSession} but serves to bridge the gap between the external, non-Vaadin data domain
 * and individual {@link com.vaadin.server.VaadinSession}s. The {@link ExternalPropertyRegistry} controls when to
 * invalidate the cached value and notify registered {@link com.vaadin.data.Property.ValueChangeListeners}s.
 * </p>
 *
 * <p>
 * Instances have an <i>external identity</i> which serves as the "back-end" address of the property and links together
 * multiple {@link ExternalProperty} instances (possibly in different {@link com.vaadin.server.VaadinSession}s) that depend
 * on the same underlying external data. This allows the {@link ExternalPropertyRegistry} to efficiently manage notifications
 * for all such linked instances: external processes trigger change notifications by providing the external identity
 * to {@link ExternalPropertyRegistry ExternalPropertyRegistry.notifyValueChanged()}, thereby remaining unaware of Vaadin
 * and if or how many {@link com.vaadin.server.VaadinSession}s currently exist, and the {@link ExternalPropertyRegistry}
 * converts these into the appropriate cache value invalidations and {@link com.vaadin.data.Property.ValueChangeListener}
 * notifications within the corresponding {@link com.vaadin.server.VaadinSession}s.
 * </p>
 *
 * @param <V> the type of the property
 * @see ExternalPropertyRegistry
 */
@SuppressWarnings("serial")
public abstract class ExternalProperty<V> extends ReadOnlyProperty<V> {

    protected final ExternalPropertyRegistry registry;

    private V cachedValue;
    private boolean valid;

    /**
     * Constructor. No initial value for the property will be set.
     *
     * @param registry registry for listener registrations
     * @throws IllegalArgumentException if {@code registry} is null
     */
    protected ExternalProperty(ExternalPropertyRegistry registry) {
        this(registry, null, false);
    }

    /**
     * Constructor. The property's value will be initially set to {@code initialValue}.
     *
     * @param registry registry for listener registrations
     * @param initialValue initial property value
     * @throws IllegalArgumentException if {@code registry} is null
     */
    protected ExternalProperty(ExternalPropertyRegistry registry, V initialValue) {
        this(registry, initialValue, true);
    }

    ExternalProperty(ExternalPropertyRegistry registry, V initialValue, boolean valid) {
        if (registry == null)
            throw new IllegalArgumentException("null registry");
        this.registry = registry;
        this.cachedValue = initialValue;
        this.valid = valid;
    }

    @Override
    public V getValue() {

        // Sanity check
        VaadinUtil.getCurrentSession();

        // Check cached value and recalculate if necessary
        if (!this.valid) {
            this.cachedValue = this.calculateValue();
            this.valid = true;
        }
        return this.cachedValue;
    }

    /**
     * Invalidate this instance's cached value, if any. This will force a {@linkplain #calculateValue recalculation}
     * of this property's value on the {@linkplain #getValue next access}.
     *
     * <p>
     * This method must be invoked within the context of the {@link com.vaadin.server.VaadinSession}
     * with which this instance is associated.
     * </p>
     */
    protected void invalidateCachedValue() {

        // Sanity check
        VaadinUtil.getCurrentSession();

        // Invalidate cached value
        this.cachedValue = null;
        this.valid = false;
    }

    /**
     * Re-calculate this instance's value.
     *
     * <p>
     * Typically this accesses some external non-Vaadin data, creating transactions and/or acquiring the appropriate
     * external locks, etc.
     * </p>
     *
     * <p>
     * This method will be invoked within the context of the {@link com.vaadin.server.VaadinSession}
     * with which this instance is associated.
     * </p>
     */
    protected abstract V calculateValue();

    /**
     * Get the external identity of this property. Instances with the same external identity (where "same" means
     * {@link Object#equals equals()}) are assumed to depend on the same external information.
     *
     * <p>
     * The returned value should be a non-null value.
     * </p>
     *
     * @see ExternalPropertyRegistry#notifyValueChanged
     */
    public abstract Object getExternalIdentity();
}

