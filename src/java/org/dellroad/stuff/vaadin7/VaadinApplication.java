
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.SessionDestroyListener;
import com.vaadin.server.VaadinServiceSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A globally accessible "Vaadin application" singleton.
 *
 * <p>
 * This class is intended to be used as a singleton in the Spring application context created by a {@link SpringServiceSession};
 * it provides a few convenience methods. Normally it would be declared in the Spring application context XML
 * file as a singleton, whereby other beans and widgets in the Vaadin application context can then autowire it and have
 * access to the methods provided here. If this class is subclassed, additional application-specific fields and methods
 * can be supplied to the entire application. In any case, the singleton instance is always accessible from any Vaadin
 * thread via {@link #get() get()} or {@link #get(Class) get(Class)}.
 *
 * @see org.dellroad.stuff.vaadin7.SpringServiceSession
 * @see com.vaadin.server.VaadinService
 */
@SuppressWarnings("serial")
public class VaadinApplication {

    /**
     * The {@link VaadinServiceSession} attribute key under which the singleton {@link VaadinApplication} instance is stored.
     */
    public static final String VAADIN_APPLICATION_ATTRIBUTE_KEY = "vaadinApplication";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * The session that this instance is associated with.
     */
    protected final VaadinServiceSession session;

    /**
     * Convenience constructor. Equivalent to:
     * <blockquote>
     *  {@link #VaadinApplication(VaadinServiceSession) VaadinApplication(VaadinUtil.getCurrentSession())}
     * </blockquote>
     *
     * @throws IllegalStateException if there is no {@link VaadinServiceSession} associated with the current thread
     */
    public VaadinApplication() {
        this(VaadinUtil.getCurrentSession());
    }

    /**
     * Constructor.
     *
     * @param session the session with which this instance should be associated
     * @throws IllegalArgumentException if {@code session} is null
     */
    public VaadinApplication(VaadinServiceSession session) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        this.session = session;
    }

    /**
     * Get the {@link VaadinServiceSession} associated with this instance.
     *
     * @return associated VaadinServiceSession, never null
     */
    public VaadinServiceSession getSession() {
        return this.session;
    }

    /**
     * Close the {@link VaadinServiceSession} associated with this instance.
     * After invoking this method, the caller would normally ensure that no further references to this
     * instance remain so that it and the associated {@link VaadinServiceSession} can be freed.
     */
    public void close() {
        this.session.removeFromSession(this.session.getService());  // TODO: update when fixed: http://dev.vaadin.com/ticket/9859
    }

    /**
     * Get the singleton instance of this class associated with the current {@link VaadinServiceSession}.
     *
     * @return singleton instance for the current Vaadin application, never null
     * @throws IllegalStateException if there is no {@link VaadinServiceSession} associated with the current thread
     * @throws IllegalStateException if there is no instance associated with the {@link VaadinServiceSession}
     * @throws IllegalStateException if the {@link #VAADIN_APPLICATION_ATTRIBUTE_KEY} attribute associated with the
     *  current {@link VaadinServiceSession} is not an instance of this class
     */
    public static VaadinApplication get() {

        // Get current session
        VaadinServiceSession session = VaadinUtil.getCurrentSession();

        // Get the associated singleton
        Object value = session.getAttribute(VAADIN_APPLICATION_ATTRIBUTE_KEY);
        if (value == null) {
            throw new IllegalStateException("there is no VaadinApplication associated with the current VaadinServiceSession;"
              + " did you declare an instance of VaadinApplication in the Vaadin Spring application context?");
        }

        // Verify type
        if (!(value instanceof VaadinApplication)) {
            throw new IllegalStateException("the `" + VAADIN_APPLICATION_ATTRIBUTE_KEY + "' attribute associated with"
              + " the current VaadinServiceSession is not an instance of " + VaadinApplication.class.getName()
              + "; instead, it is a " + value.getClass().getName());
        }

        // Done
        return (VaadinApplication)value;
    }

    /**
     * Get the singleton instance of this class associated with the current {@link VaadinServiceSession},
     * cast to the given type.
     *
     * @return singleton instance for the current Vaadin application, never null
     * @throws IllegalStateException if there is no {@link VaadinServiceSession} associated with the current thread
     * @throws IllegalStateException if there is no instance associated with the {@link VaadinServiceSession}
     * @throws IllegalStateException if the {@link #VAADIN_APPLICATION_ATTRIBUTE_KEY} attribute associated with the
     *  current {@link VaadinServiceSession} is not an instance of this class
     * @throws IllegalArgumentException if the {@code type} is null
     */
    public static <T extends VaadinApplication> T get(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        return type.cast(VaadinApplication.get());
    }

    /**
     * Peform some action while holding the lock of the {@link VaadinServiceSession} associated with this instance.
     *
     * <p>
     * This is a convenience method that in turn invokes {@link VaadinUtil#invoke VaadinUtil.invoke()} using the
     * {@link VaadinServiceSession} associated with this instance.
     *
     * @param action action to perform
     * @throws IllegalArgumentException if {@code action} is null
     * @see VaadinUtil#invoke
     */
    public void invoke(Runnable action) {
        VaadinUtil.invoke(this.session, action);
    }

    /**
     * Register for a notification when the {@link VaadinServiceSession} is closed, without creating a memory leak.
     *
     * <p>
     * This is a convenience method that in turn invokes
     * {@link VaadinUtil#addSessionDestroyListener VaadinUtil.addSessionDestroyListener()} using the
     * {@link VaadinServiceSession} associated with this instance.
     *
     * @throws IllegalArgumentException if {@code listener} is null
     */
    public void addSessionDestroyListener(SessionDestroyListener listener) {
        VaadinUtil.addSessionDestroyListener(this.session, listener);
    }

    /**
     * Remove a listener added via {@link #addSessionDestroyListener addSessionDestroyListener()}.
     *
     * <p>
     * This is a convenience method that in turn invokes
     * {@link VaadinUtil#removeSessionDestroyListener VaadinUtil.removeSessionDestroyListener()} using the
     * {@link VaadinServiceSession} associated with this instance.
     *
     * @throws IllegalArgumentException if {@code listener} is null
     */
    public void removeSessionDestroyListener(SessionDestroyListener listener) {
        VaadinUtil.removeSessionDestroyListener(this.session, listener);
    }
}

