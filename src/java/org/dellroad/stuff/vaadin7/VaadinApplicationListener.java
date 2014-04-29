
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.VaadinSession;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationEventMulticaster;

/**
 * A Spring {@link org.springframework.context.ApplicationListener} support superclass customized for use by
 * listeners that are part of a Vaadin application when listening to non-Vaadin application event sources.
 *
 * <p>
 * Listeners that are part of a Vaadin application should use this superclass if they are going to be registered
 * with non-Vaadin event sources. This will ensure that events are delivered in the proper Vaadin application context
 * and memory leaks are avoided when the session closes.
 * </p>
 *
 * @param <E> The type of the event
 * @see VaadinExternalListener
 * @see VaadinUtil#invoke
 * @see VaadinApplicationScope
 * @see SpringVaadinSessionListener
 */
public abstract class VaadinApplicationListener<E extends ApplicationEvent>
  extends VaadinExternalListener<ApplicationEventMulticaster> implements ApplicationListener<E> {

    /**
     * Convenience constructor. Equivalent to:
     * <blockquote>
     *  {@link #VaadinApplicationListener(ApplicationEventMulticaster, VaadinSession)
     *      VaadinApplicationListener(multicaster, VaadinUtil.getCurrentSession())}
     * </blockquote>
     *
     * @param multicaster the application event multicaster on which this listener will register
     * @throws IllegalArgumentException if {@code multicaster} is null
     * @throws IllegalArgumentException if there is no {@link VaadinSession} associated with the current thread
     */
    public VaadinApplicationListener(ApplicationEventMulticaster multicaster) {
        this(multicaster, VaadinUtil.getCurrentSession());
    }

    /**
     * Primary constructor.
     *
     * @param multicaster the application event multicaster on which this listener will register
     * @param session the associated Vaadin application
     * @throws IllegalArgumentException if either parameter is null
     */
    public VaadinApplicationListener(ApplicationEventMulticaster multicaster, VaadinSession session) {
        super(multicaster, session);
    }

    @Override
    protected void register(ApplicationEventMulticaster multicaster) {
        multicaster.addApplicationListener(this);
    }

    @Override
    protected void unregister(ApplicationEventMulticaster multicaster) {
        multicaster.removeApplicationListener(this);
    }

    @Override
    public final void onApplicationEvent(final E event) {
        this.handleEvent(new Runnable() {
            @Override
            public void run() {
                VaadinApplicationListener.this.onApplicationEventInternal(event);
            }
        });
    }

    /**
     * Handle a listener event. When this method is invoked, it will already be within the context
     * of the {@link VaadinSession} with which this listener is associated.
     *
     * @see VaadinUtil#invoke
     * @see VaadinUtil#getCurrentSession
     */
    protected abstract void onApplicationEventInternal(E event);
}

