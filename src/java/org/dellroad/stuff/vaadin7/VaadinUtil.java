
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.SessionDestroyEvent;
import com.vaadin.server.SessionDestroyListener;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinService;
import com.vaadin.server.VaadinSession;

import java.util.concurrent.Future;

/**
 * Miscellaneous utility methods.
 */
public final class VaadinUtil {

    private VaadinUtil() {
    }

    /**
     * Verify that we are running in the context of the given session and holding the session's lock.
     * This method can be used by any code that manipulates Vaadin state to assert that the proper Vaadin
     * locking has been performed.
     *
     * @param session session we are supposed to be running with
     * @throws IllegalArgumentException if {@code session} is null
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     * @throws IllegalStateException if the {@link VaadinSession} associated with the current thread is not {@code session}
     * @throws IllegalStateException if the {@link VaadinSession} associated with the current thread is not locked
     * @throws IllegalStateException if the {@link VaadinSession} associated with the current thread is locked by another thread
     */
    public static void assertSession(VaadinSession session) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        final VaadinSession currentSession = VaadinSession.getCurrent();
        if (currentSession == null)
            throw new IllegalStateException("there is no VaadinSession associated with the current thread");
        if (currentSession != session) {
            throw new IllegalStateException("the VaadinSession associated with the current thread " + currentSession
              + " is not the same session as the given one " + session);
        }
        if (!session.hasLock()) {
            throw new IllegalStateException("the VaadinSession associated with the current thread " + currentSession
              + " is not locked by this thread");
        }
    }

    /**
     * Get the {@link VaadinSession} associated with the current thread.
     * This is just a wrapper around {@link VaadinSession#getCurrent} that throws an exception instead
     * of returning null when there is no session associated with the current thread.
     *
     * @return current {@link VaadinSession}, never null
     *
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     */
    public static VaadinSession getCurrentSession() {
        VaadinSession session = VaadinSession.getCurrent();
        if (session == null) {
            throw new IllegalStateException("there is no VaadinSession associated with the current thread;"
              + " are we executing within a Vaadin HTTP request or VaadinUtil.invoke()?");
        }
        return session;
    }

    /**
     * Get the {@link VaadinRequest} associated with the current thread.
     * This is just a wrapper around {@link VaadinService#getCurrentRequest} that throws an exception instead
     * of returning null when there is no request associated with the current thread.
     *
     * @return current {@link VaadinRequest}, never null
     *
     * @throws IllegalStateException if there is no {@link VaadinRequest} associated with the current thread
     */
    public static VaadinRequest getCurrentRequest() {
        VaadinRequest request = VaadinService.getCurrentRequest();
        if (request == null) {
            throw new IllegalStateException("there is no VaadinRequest associated with the current thread;"
              + " are we executing within a Vaadin HTTP request?");
        }
        return request;
    }

    /**
     * Peform some action while holding the given {@link VaadinSession}'s lock.
     *
     * <p>
     * This method now just invokes {@link VaadinSession#accessSynchronously}, a method which didn't exist in earlier
     * versions of Vaadin.
     * </p>
     *
     * <p>
     * All back-end threads that interact with Vaadin components must use this method (or {@link #invokeLater invokeLater()})
     * to avoid race conditions. Since session locks are re-entrant, it will not cause problems if this method is also
     * used by a "front-end" (i.e., Vaadin HTTP request) thread.
     * </p>
     *
     * <p>
     * Note: when executing within a Vaadin HTTP request, the current thread's {@link VaadinSession} is available
     * via {@link VaadinSession#getCurrent}; consider also using {@link VaadinApplication#invoke} instead of this method.
     * </p>
     *
     * <p>
     * <b>Warning:</b> background threads should be careful when invoking this method to ensure they
     * are not already holding an application-specific lock that a separate HTTP request thread could
     * attempt to acquire during its normal processing: because the HTTP request thread will probably
     * already be holding the session lock when it attempts to acquire the application-specific lock,
     * this creates the potential for a lock-ordering reversal deadlock.
     * </p>
     *
     * @throws IllegalArgumentException if either parameter is null
     * @see VaadinApplication#invoke
     */
    public static void invoke(VaadinSession session, Runnable action) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        if (action == null)
            throw new IllegalArgumentException("null action");
        session.accessSynchronously(action);
    }

    /**
     * Peform some action while holding the given {@link VaadinSession}'s lock, but do so asynchronously.
     *
     * <p>
     * Here the term "asynchronously" means:
     * <ul>
     *  <li>If any thread holds the session lock (including the current thread), this method will return
     *      immediately and the action will be performed later, when the session is eventually unlocked.</li>
     *  <li>If no thread holds the session lock, the session will be locked and the action performed
     *      synchronously by the current thread.</li>
     * </ul>
     * </p>
     *
     * <p>
     * This method now just invokes {@link VaadinSession#access}, a method which didn't exist in earlier
     * versions of Vaadin.
     * </p>
     *
     * <p>
     * Note: when executing within a Vaadin HTTP request, the current thread's {@link VaadinSession} is available
     * via {@link VaadinSession#getCurrent}; consider also using {@link VaadinApplication#invokeLater} instead of this method.
     * </p>
     *
     * @return a corresponding {@link Future}
     * @throws IllegalArgumentException if either parameter is null
     * @see #invoke
     * @see VaadinApplication#invokeLater
     */
    public static Future<Void> invokeLater(VaadinSession session, Runnable action) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        if (action == null)
            throw new IllegalArgumentException("null action");
        return session.access(action);
    }

    /**
     * Register for a notification when the {@link VaadinSession} is closed, without creating a memory leak.
     * This method is intended to be used by listeners that are themselves part of a Vaadin application.
     *
     * <p>
     * Explanation: the {@link VaadinSession} class does not provide a listener API directly; instead, you must
     * use the {@link com.vaadin.server.VaadinService} class. However, registering as a listener on the
     * {@link com.vaadin.server.VaadinService} when you are part of a Vaadin application sets you up for a memory leak
     * if you forget to unregister yourself when the notification arrives, because the {@link com.vaadin.server.VaadinService}
     * lifetime is longer than the {@link VaadinSession} lifetime. This method handles that de-registration for
     * you automatically.
     *
     * @throws IllegalArgumentException if either parameter is null
     * @see VaadinApplication#addSessionDestroyListener
     */
    public static void addSessionDestroyListener(VaadinSession session, SessionDestroyListener listener) {
        session.getService().addSessionDestroyListener(new LeakAvoidingDestroyListener(session, listener));
    }

    /**
     * Remove a listener added via {@link #addSessionDestroyListener addSessionDestroyListener()}.
     *
     * @throws IllegalArgumentException if either parameter is null
     * @see VaadinApplication#removeSessionDestroyListener
     */
    public static void removeSessionDestroyListener(VaadinSession session, SessionDestroyListener listener) {
        session.getService().removeSessionDestroyListener(new LeakAvoidingDestroyListener(session, listener));
    }

// LeakAvoidingDestroyListener

    @SuppressWarnings("serial")
    private static class LeakAvoidingDestroyListener implements SessionDestroyListener {

        private final VaadinSession session;
        private final SessionDestroyListener listener;

        public LeakAvoidingDestroyListener(VaadinSession session, SessionDestroyListener listener) {
            if (session == null)
                throw new IllegalArgumentException("null session");
            if (listener == null)
                throw new IllegalArgumentException("null listener");
            this.session = session;
            this.listener = listener;
        }

        @Override
        public void sessionDestroy(SessionDestroyEvent event) {
            final VaadinSession closedSession = event.getSession();
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

