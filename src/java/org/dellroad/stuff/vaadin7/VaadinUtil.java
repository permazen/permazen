
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.SessionDestroyEvent;
import com.vaadin.server.SessionDestroyListener;
import com.vaadin.server.VaadinServiceSession;

/**
 * Miscellaneous utility methods.
 */
public final class VaadinUtil {

    private VaadinUtil() {
    }

    /**
     * Get the {@link VaadinServiceSession} associated with the current thread.
     * This is just a wrapper around {@link VaadinServiceSession#getCurrent()} that throws an exception instead
     * of returning null when there is no session associated with the current thread.
     *
     * @return current {@link VaadinServiceSession}, never null
     *
     * @throws IllegalStateException if there is no {@link VaadinServiceSession} associated with the current thread
     */
    public static VaadinServiceSession getCurrentSession() {
        VaadinServiceSession session = VaadinServiceSession.getCurrent();
        if (session == null) {
            throw new IllegalStateException("there is no VaadinServiceSession associated with the current thread;"
              + " are we executing within a Vaadin HTTP request or VaadinUtil.invoke()?");
        }
        return session;
    }

    /**
     * Peform some action while holding the given {@link VaadinServiceSession}'s lock.
     *
     * <p>
     * All back-end threads that interact with Vaadin components must use this method (or equivalent)
     * to avoid race conditions. Since session locks are re-entrant, it will not cause problems if this
     * method is used by a "front-end" (i.e., Vaadin HTTP request) thread.
     * </p>
     *
     * <p>
     * Note: when executing within a Vaadin HTTP request, the current thread's {@link VaadinServiceSession}
     * is available via {@link VaadinServiceSession#getCurrent}.
     * </p>
     *
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void invoke(VaadinServiceSession session, Runnable action) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        if (action == null)
            throw new IllegalArgumentException("null action");
        session.getLock().lock();
        try {
            action.run();
        } finally {
            session.getLock().unlock();
        }
    }

    /**
     * Register for a notification when the {@link VaadinServiceSession} is closed, without creating a memory leak.
     *
     * <p>
     * Explanation: the {@link VaadinServiceSession} class does not provide a listener API directly; instead, you must
     * use the {@link com.vaadin.server.VaadinService} class. However, registering as a listener on the
     * {@link com.vaadin.server.VaadinService} sets you up for a memory leak if you forget to unregister yourself
     * when the notification arrives, because the {@link com.vaadin.server.VaadinService} lifetime is longer than the
     * {@link VaadinServiceSession} lifetime. This method handles that de-registration for you.
     *
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void addSessionDestroyListener(VaadinServiceSession session, SessionDestroyListener listener) {
        session.getService().addSessionDestroyListener(new LeakAvoidingDestroyListener(session, listener));
    }

    /**
     * Remove a listener added via {@link #addSessionDestroyListener addSessionDestroyListener()}.
     *
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void removeSessionDestroyListener(VaadinServiceSession session, SessionDestroyListener listener) {
        session.getService().removeSessionDestroyListener(new LeakAvoidingDestroyListener(session, listener));
    }

// LeakAvoidingDestroyListener

    private static class LeakAvoidingDestroyListener implements SessionDestroyListener {

        private final VaadinServiceSession session;
        private final SessionDestroyListener listener;

        public LeakAvoidingDestroyListener(VaadinServiceSession session, SessionDestroyListener listener) {
            if (session == null)
                throw new IllegalArgumentException("null session");
            if (listener == null)
                throw new IllegalArgumentException("null listener");
            this.session = session;
            this.listener = listener;
        }

        @Override
        public void sessionDestroy(SessionDestroyEvent event) {
            final VaadinServiceSession closedSession = event.getSession();
            if (closedSession == this.session) {
                this.session.getService().removeSessionDestroyListener(this);       // remove myself as listener to avoid mem leak
                this.listener.sessionDestroy(event);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            LeakAvoidingDestroyListener that = (LeakAvoidingDestroyListener)obj;
            return this.session == that.session && this.listener.equals(that.listener);
        }

        @Override
        public int hashCode() {
            return this.session.hashCode() ^ this.listener.hashCode();
        }
    }
}

