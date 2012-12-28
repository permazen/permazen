
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SmartApplicationListener;

/**
 * A Spring {@link org.springframework.context.ApplicationListener} support superclass customized for use by
 * listeners that are part of a Vaadin application when listening to non-Vaadin application event sources.
 *
 * <p>
 * Listeners that are part of a Vaadin application should use this superclass if they are going to be registered
 * with non-Vaadin event multicasters. This will ensure that events are delivered {@linkplain ContextApplication#invoke
 * in the proper Vaadin application context} and memory leaks are avoided.
 * </p>
 *
 * @param <E> The type of the event
 * @see VaadinExternalListener
 * @see ContextApplication#invoke
 * @see VaadinApplicationScope
 * @see SpringContextApplication
 */
public abstract class VaadinApplicationListener<E extends ApplicationEvent>
  extends VaadinExternalListener<ApplicationEventMulticaster> implements SmartApplicationListener {

    private final Class<E> eventType;

    /**
     * Convenience constructor. Equivalent to:
     * <blockquote>
     *  {@link #VaadinApplicationListener(ApplicationEventMulticaster, Class, ContextApplication)
     *      VaadinApplicationListener(multicaster, eventType, ContextApplication.get())}
     * </blockquote>
     *
     * @param multicaster the application event multicaster on which this listener will register
     * @throws IllegalArgumentException if {@code eventType} is null
     * @throws IllegalStateException if there is no {@link ContextApplication} associated with the current thread
     */
    public VaadinApplicationListener(ApplicationEventMulticaster multicaster, Class<E> eventType) {
        this(multicaster, eventType, ContextApplication.get());
    }

    /**
     * Primary constructor. This results in this instance being registered as a listener on {@code multicaster}.
     *
     * @param multicaster the application event multicaster on which this listener will register
     * @param eventType type of events this instance should receive (others will be ignored)
     * @param application the associated Vaadin application
     * @throws IllegalArgumentException if either parameter is null
     */
    public VaadinApplicationListener(ApplicationEventMulticaster multicaster, Class<E> eventType, ContextApplication application) {
        super(multicaster, application);
        if (eventType == null)
            throw new IllegalArgumentException("null eventType");
        this.eventType = eventType;
    }

    /**
     * Get the event type that this listener listens for.
     */
    public final Class<E> getEventType() {
        return this.eventType;
    }

    @Override
    protected void register(ApplicationEventMulticaster eventSource) {
        this.getEventSource().addApplicationListener(this);
    }

    @Override
    protected void unregister(ApplicationEventMulticaster eventSource) {
        this.getEventSource().removeApplicationListener(this);
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
        this.handleEvent(new Runnable() {
            @Override
            public void run() {
                VaadinApplicationListener.this.onApplicationEventInternal(castEvent2);
            }
        });
    }

    /**
     * Handle a listener event. When this method is invoked, it will already be within the context
     * of the {@link ContextApplication} with which this listener is associated.
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
    public boolean supportsEventType(Class<? extends ApplicationEvent> eventType) {
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

