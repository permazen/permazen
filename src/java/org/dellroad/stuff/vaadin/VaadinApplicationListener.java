
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
 * listeners that are part of a Vaadin application when listening to non-Vaadin application event sources.
 *
 * <p>
 * Listeners that are part of a Vaadin application should use this superclass if they are going to be registered
 * with non-Vaadin event sources. This will ensure that events are delivered {@linkplain ContextApplication#invoke
 * in the proper Vaadin application context}.
 * </p>
 *
 * <p>
 * Note: to avoid memory leaks, listeners must be explicitly unregistered when the associated Vaadin application closes.
 * This can be done by explicitly {@linkplain ContextApplication#addListener registering for an application close notification}
 * in which the listener is unregistered, or by unregistering the listener in the Spring
 * {@linkplain org.springframework.beans.factory.DisposableBean#destroy destroy-method} associated with a
 * bean that has {@link VaadinApplicationScope scope="vaadinApplication"} or lives in a {@link SpringContextApplication}
 * application context (so that the bean's destroy method will be invoked when the Vaadin application closes).
 * </p>
 *
 * <p>
 * Note: for event sources that are scoped to specific Vaadin application instances and will deliver events
 * within the proper Vaadin application context, then the use of this listener superclass is not necessary.
 * </p>
 *
 * @see ContextApplication#invoke
 * @see VaadinApplicationScope
 * @see SpringContextApplication
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
     * Handle a listener event within the context of the {@link ContextApplication} with which this listener is associated.
     * The current {@link ContextApplication} is also available via {@link ContextApplication#get}.
     *
     * @see ContextApplication#get
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

