
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.VaadinSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Central clearing house for invalidating cached values and issuing change notification to listeners for all
 * {@link ExternalProperty}s that depend on external data. The underlying data for any particular {@link ExternalProperty}
 * is identified by its {@linkplain ExternalProperty#getExternalIdentity external identity}. When this value is
 * provided to {@link #notifyValueChanged notifyValueChanged()}, the corresponding {@link ExternalProperty}s have
 * their cached values invalidated (optionally recalculating it preemptively) and their change listeners notified.
 *
 * @see ExternalProperty
 */
@SuppressWarnings("serial")
public class ExternalPropertyRegistry {

    private final WeakHashMap<VaadinSession, HashMap<Object, HashSet<ExternalProperty<?>>>> sessionMap = new WeakHashMap<>();

    /**
     * Invalidate cached values and notify {@link com.vaadin.data.Property.ValueChangeListener}s for all registered
     * {@link ExternalProperty}s having the specified external identity.
     *
     * <p>
     * If {@code recalculate} is true, after invalidating cached values, {@link #recalculateCachedValues recalculateCachedValues()}
     * will be invoked to optimistically pre-load updated values into each property.
     * </p>
     *
     * <p>
     * This method should <b>not</b> be invoked within the context of a {@link VaadinSession}.
     * </p>
     *
     * <p>
     * This method will return immediately; notification of {@link ExternalProperty} listeners is performed asynchronously.
     * </p>
     *
     * @param id external identity
     * @param recalculate if true, go ahead and {@linkplain ExternalProperty#calculateValue recalculate}
     *  new property values as well
     */
    public synchronized void notifyValueChanged(Object id, final boolean recalculate) {
        for (Map.Entry<VaadinSession, HashMap<Object, HashSet<ExternalProperty<?>>>> entry : this.sessionMap.entrySet()) {
            final HashMap<Object, HashSet<ExternalProperty<?>>> propertyMap = entry.getValue();
            final HashSet<ExternalProperty<?>> properties = entry.getValue().get(id);
            if (properties == null)
                continue;
            entry.getKey().access(new Runnable() {
                @Override
                public void run() {
                    ExternalPropertyRegistry.this.notifyValueChanged(new ArrayList<ExternalProperty<?>>(properties), recalculate);
                }
            });
        }
    }

    // Invoked within session context
    private void notifyValueChanged(ArrayList<ExternalProperty<?>> properties, boolean recalculate) {
        VaadinUtil.getCurrentSession();

        // Invalidate cached values
        for (ExternalProperty<?> property : properties)
            property.invalidateCachedValue();

        // Recalculate invalidated values
        if (recalculate)
            this.recalculateCachedValues(properties);

        // Fire change notifications
        for (ExternalProperty<?> property : properties)
            property.fireValueChange();
    }

    /**
     * Recalculate property values for the given properties which are associated with the current {@link VaadinSession}
     * and which have just had their cached values invalidated.
     *
     * <p>
     * This method is invoked (indirectly and asynchronously) by {@link #notifyValueChanged notifyValueChanged()}
     * when the {@code recalculate} parameter is true to preemptively recalculate property values (i.e., pre-load the cache).
     * </p>
     *
     * <p>
     * The implementation in {@link ExternalPropertyRegistry} just invokes {@link ExternalProperty#getValue}
     * on each property. Subclasses for which this would create an individual transaction for each property
     * may instead want to override this method to create a single consolidated transaction.
     * </p>
     */
    protected void recalculateCachedValues(List<ExternalProperty<?>> properties) {
        for (ExternalProperty<?> property : properties)
            property.getValue();
    }

    /**
     * Add a property to this registry.
     *
     * <p>
     * This method is invoked when needed by {@link ExternalProperty#addValueChangeListener}.
     * </p>
     *
     * @throws IllegalArgumentException if {@code property} is null
     * @throws IllegalStateException if there is no current {@link VaadinSession}
     */
    protected synchronized void add(ExternalProperty<?> property) {
        if (property == null)
            throw new IllegalArgumentException("null property");
        final Object id = property.getExternalIdentity();
        final VaadinSession session = VaadinUtil.getCurrentSession();
        HashMap<Object, HashSet<ExternalProperty<?>>> propertyMap = this.sessionMap.get(session);
        if (propertyMap == null) {
            propertyMap = new HashMap<>(1);
            this.sessionMap.put(session, propertyMap);
        }
        HashSet<ExternalProperty<?>> properties = propertyMap.get(id);
        if (properties == null) {
            properties = new HashSet<>(1);
            propertyMap.put(id, properties);
        }
        properties.add(property);
    }

    /**
     * Remove a property from this registry.
     *
     * <p>
     * This method is invoked when needed by {@link ExternalProperty#removeValueChangeListener}.
     * </p>
     *
     * @throws IllegalArgumentException if {@code property} is null
     * @throws IllegalStateException if there is no current {@link VaadinSession}
     */
    protected synchronized void remove(ExternalProperty<?> property) {
        if (property == null)
            throw new IllegalArgumentException("null property");
        final Object id = property.getExternalIdentity();
        final VaadinSession session = VaadinUtil.getCurrentSession();
        final HashMap<Object, HashSet<ExternalProperty<?>>> propertyMap = this.sessionMap.get(session);
        if (propertyMap == null)
            return;
        final HashSet<ExternalProperty<?>> properties = propertyMap.get(id);
        if (properties == null)
            return;
        if (properties.remove(property) && properties.isEmpty() && propertyMap.remove(id) != null && propertyMap.isEmpty())
            this.sessionMap.remove(session);
    }
}

