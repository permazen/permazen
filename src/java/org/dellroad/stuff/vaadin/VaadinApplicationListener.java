
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.SmartApplicationListener;

/**
 * A Spring {@link org.springframework.context.ApplicationListener} support superclass customized for use by
 * objects that are part of a Vaadin application.
 *
 * <p>
 * Objects that are part of a Vaadin application should use this superclass for any listeners that are going to
 * be registered with non-Vaadin event sources. This will ensure that the listener is invoked with proper
 * application synchronization, {@linkplain VaadinApplicationScope scoping}, etc.
 * </p>
 *
 * <p>
 * In addition, to avoid memory leaks, such listeners must be unregistered when the application closes.
 * This can be done by {@link ContextApplication#addListener registering for an application close notification}
 * or by defining the listener with {@link VaadinApplicationScope scope="vaadinApplication"} and a Spring
 * {@linkplain org.springframework.beans.factory.DisposableBean#destroy destroy-method} that unregisters itself.
 * </p>
 *
 * @see ContextApplication#invoke
 * @see VaadinApplicationScope
 */
public abstract class VaadinApplicationListener<E extends ApplicationEvent> implements SmartApplicationListener {

    private final Class<E> eventType;
    private final ContextApplication application;

    /**
     * Convenience constructor. Equivalent to:
     * <blockquote>
     *  {@link #VaadinApplicationListener(Class, ContextApplication) VaadinApplicationListener(eventType, ContextApplication.get())}
     * </blockquote>
     *
     * @throws IllegalArgumentException if {@code eventType} is null
     * @throws IllegalStateException if there is no {@link ContextApplication} associated with the current thread
     */
    public VaadinApplicationListener(Class<E> eventType) {
        this(eventType, ContextApplication.get());
    }

    /**
     * Primary constructor.
     *
     * @param eventType type of events this instance should receive (others will be ignored)
     * @param application the associated Vaadin application
     * @throws IllegalArgumentException if either parameter is null
     */
    public VaadinApplicationListener(Class<E> eventType, ContextApplication application) {
        if (eventType == null)
            throw new IllegalArgumentException("null eventType");
        if (application == null)
            throw new IllegalArgumentException("null application");
        this.eventType = eventType;
        this.application = application;
    }

    public final Class<E> getEventType() {
        return this.eventType;
    }

    public final ContextApplication getApplication() {
        return this.application;
    }

    @Override
    public final void onApplicationEvent(ApplicationEvent event) {
        E castEvent;
        try {
            castEvent = this.eventType.cast(event);
        } catch (ClassCastException e) {
            // should not happen
            return;
        }
        final E castEvent2 = castEvent;
        this.application.invoke(new Runnable() {
            @Override
            public void run() {
                VaadinApplicationListener.this.onApplicationEventInternal(castEvent2);
            }
        });
    }

    /**
     * Handle an event in the application.
     */
    protected abstract void onApplicationEventInternal(E event);

    /**
     * Determine whether this listener actually supports the given event type.
     *
     * <p>
     * The implementation in {@link VaadinApplicationListener} tests whether {@code eventType}
     * is assignable to the type given in the constructor. Subclasses may override as desired.
     */
    @Override
    public boolean supportsEventType(@SuppressWarnings("hiding") Class<? extends ApplicationEvent> eventType) {
        return this.eventType.isAssignableFrom(eventType);
    }

    /**
     * Determine whether this listener actually supports the given source type.
     *
     * <p>
     * The implementation in {@link VaadinApplicationListener} always returns true. Subclasses may override as desired.
     */
    @Override
    public boolean supportsSourceType(Class<?> sourceType) {
        return true;
    }

    /**
     * Get ordering value.
     *
     * <p>
     * The implementation in {@link VaadinApplicationListener} always returns zero. Subclasses may override as desired.
     */
    @Override
    public int getOrder() {
        return 0;
    }
}

