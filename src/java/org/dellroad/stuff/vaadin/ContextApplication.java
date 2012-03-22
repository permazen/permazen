
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.Application;
import com.vaadin.terminal.Terminal;
import com.vaadin.terminal.gwt.server.HttpServletRequestListener;
import com.vaadin.ui.Window;

import java.net.SocketException;
import java.util.EventObject;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Application} subclass that provides some basic infrastructure for Vaadin applications:
 * <ul>
 *  <li>Access to the currently running Vaadin application (via a {@link ContextApplication#get()})</li>
 *  <li>A way to safely interact with a Vaadin application from a background thread (via {@link ContextApplication#invoke})</li>
 *  <li>Support for Vaadin {@linkplain ContextApplication#addListener application close event notifications}</li>
 *  <li>Displays any exceptions thrown in an overlay error window</li>
 *  <li>A {@link Logger} to use</li>
 * </ul>
 *
 * @since 1.0.134
 */
@SuppressWarnings("serial")
public abstract class ContextApplication extends Application implements HttpServletRequestListener {

    /**
     * Default notification linger time for error notifications (in milliseconds): Value is {@value}ms.
     */
    public static final int DEFAULT_NOTIFICATION_DELAY = 30000;

    private static final ThreadLocal<ContextApplication> CURRENT_APPLICATION = new ThreadLocal<ContextApplication>();
    private static final ThreadLocal<HttpServletRequest> CURRENT_REQUEST = new ThreadLocal<HttpServletRequest>();
    private static final ThreadLocal<HttpServletResponse> CURRENT_RESPONSE = new ThreadLocal<HttpServletResponse>();

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final HashSet<CloseListener> closeListeners = new HashSet<CloseListener>();

    private volatile ExecutorService executorService;

// Initialization

    /**
     * Initialize the application.
     *
     * <p>
     * The implementation in {@link ContextApplication} delegates to {@link #initApplication}.
     * </p>
     */
    @Override
    public final void init() {

        // Set current application
        CURRENT_APPLICATION.set(this);

        // Create executor service
        this.executorService = Executors.newSingleThreadExecutor();

        // Initialize application
        boolean initialized = false;
        try {
            this.initApplication();
            initialized = true;
        } finally {
            if (!initialized)
                this.shutdownExecutorService();
        }
    }

    /**
     * Initialize the application. Sub-classes of {@link ContextApplication} must implement this method.
     */
    protected abstract void initApplication();

// Error handling

    /**
     * Handle an uncaugt exception thrown by a Vaadin HTTP request.
     *
     * <p>
     * The implementation in {@link ContextApplication} logs the error and displays in on the
     * user's screen via {@link #showError(String, Throwable)}.
     */
    @Override
    public void terminalError(Terminal.ErrorEvent event) {

        // Delegate to superclass
        super.terminalError(event);

        // Get exception; ignore client "hangups"
        final Throwable t = event.getThrowable();
        if (t instanceof SocketException)
            return;

        // Notify user and log it
        this.showError("Internal Error", "" + t);
        this.log.error("error within Vaadin operation", t);
    }

    /**
     * Display an error message to the user.
     */
    public void showError(String title, String description) {
        Window.Notification notification = new Window.Notification(title, description, Window.Notification.TYPE_ERROR_MESSAGE);
        notification.setStyleName("warning");
        notification.setDelayMsec(this.getNotificationDelay());
        this.getMainWindow().showNotification(notification);
    }

    /**
     * Display an error message to the user caused by an exception.
     */
    public void showError(String title, Throwable t) {
        for (int i = 0; i < 100 && t.getCause() != null; i++)
            t = t.getCause();
        this.showError(title, this.getErrorMessage(t));
    }

    /**
     * Get the notification linger time for error notifications (in milliseconds).
     *
     * <p>
     * The implementation in {@link ContextApplication} returns {@link #DEFAULT_NOTIFICATION_DELAY}.
     */
    protected int getNotificationDelay() {
        return DEFAULT_NOTIFICATION_DELAY;
    }

    /**
     * Convert an exception into a displayable error message.
     */
    protected String getErrorMessage(Throwable t) {
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }

// ThreadLocal stuff

    /**
     * Set this instance as the "current application" (if not set already) while invoking the given callback.
     *
     * <p>
     * This method is useful for situations in which non-Vaadin threads need to call into Vaadin code
     * that expects to successfully retrieve the current application via {@link #currentApplication}.
     * </p>
     *
     * <p>
     * This method also synchronizes on this {@link ContextApplication} instance as required by Vaadin for thread safety.
     * </p>
     *
     * @param action action to perform
     * @throws IllegalStateException if a different {@link ContextApplication} is already set as the current application
     *  associated with the current thread
     * @see #invokeLater invokeLater()
     */
    public void invoke(Runnable action) {
        final ContextApplication previous = ContextApplication.CURRENT_APPLICATION.get();
        if (previous != null && previous != this)
            throw new IllegalStateException("there is already a current application for this thread");
        ContextApplication.CURRENT_APPLICATION.set(this);
        try {
            synchronized (this) {
                action.run();
            }
        } finally {
            ContextApplication.CURRENT_APPLICATION.set(previous);
        }
    }

    /**
     * Set this instance as the "current application" and invoke the given callback from within another thread.
     *
     * <p>
     * This method functions like {@like #invoke invoke()} except that {@code action} will be invoked from within
     * a separate thread dedicated to this application instance. This is useful to reduce Vaadin application lock
     * contention, by performing Vaadin-related actions in a separate, dedicated thread. Actions are executed
     * in the order they are given to this method.
     * </p>
     *
     * <p>
     * The returned {@link Future}'s {@link Future#get get()} method will return null upon successful completion.
     * This method itself always returns immediately.
     * </p>
     *
     * @param action action to perform
     * @return a {@link Future} representing the pending results of {@code action}
     * @throws IllegalStateException if this instance is not initialized or has been closed
     * @see #invoke invoke()
     */
    public Future<?> invokeLater(Runnable action) {
        ExecutorService executor = this.executorService;
        if (executor == null)
            throw new IllegalStateException("application instance is either not initialized or already closed");
        return executor.submit(new LaterRunnable(action));
    }

    /**
     * Handle the start of a request.
     *
     * <p>
     * The implementation in {@link ContextApplication} delegates to {@link #doOnRequestStart}.
     * </p>
     */
    @Override
    public final void onRequestStart(HttpServletRequest request, HttpServletResponse response) {
        ContextApplication.CURRENT_APPLICATION.set(this);
        ContextApplication.CURRENT_REQUEST.set(request);
        ContextApplication.CURRENT_RESPONSE.set(response);
        this.doOnRequestStart(request, response);
    }

    /**
     * Handle the end of a request.
     *
     * <p>
     * The implementation in {@link ContextApplication} delegates to {@link #doOnRequestEnd}.
     * </p>
     */
    @Override
    public final void onRequestEnd(HttpServletRequest request, HttpServletResponse response) {
        try {
            this.doOnRequestEnd(request, response);
        } finally {
            ContextApplication.CURRENT_APPLICATION.remove();
            ContextApplication.CURRENT_REQUEST.remove();
            ContextApplication.CURRENT_RESPONSE.remove();
        }
    }

    /**
     * Sub-class hook for handling the start of a request. This method is invoked by {@link #onRequestStart}.
     *
     * <p>
     * The implementation in {@link ContextApplication} does nothing. Subclasses should override as necessary.
     * </p>
     */
    protected void doOnRequestStart(HttpServletRequest request, HttpServletResponse response) {
    }

    /**
     * Sub-class hook for handling the end of a request. This method is invoked by {@link #onRequestEnd}.
     *
     * <p>
     * The implementation in {@link ContextApplication} does nothing. Subclasses should override as necessary.
     * </p>
     */
    protected void doOnRequestEnd(HttpServletRequest request, HttpServletResponse response) {
    }

    /**
     * Get the {@link ContextApplication} instance associated with the current thread.
     *
     * <p>
     * If the current thread is handling a Vaadin HTTP request that is executing within an {@link ContextApplication} instance,
     * or is executing within {@link #invoke}, then this method will return the associated {@link ContextApplication}.
     * </p>
     *
     * @return the {@link ContextApplication} associated with the current thread, or {@code null} if the current thread
     *  is not servicing a Vaadin web request or the current Vaadin {@link Application} is not an {@link ContextApplication}
     *
     * @see #invoke
     */
    public static ContextApplication currentApplication() {
        return ContextApplication.CURRENT_APPLICATION.get();
    }

    /**
     * Get the {@link ContextApplication} associated with the current thread, cast to the desired type.
     *
     * @param type expected application type
     * @return the {@link ContextApplication} associated with the current thread
     * @see #currentApplication()
     * @throws ClassCastException if the current application is not assignable to {@code type}
     */
    public static <A extends ContextApplication> A currentApplication(Class<A> type) {
        return type.cast(ContextApplication.currentApplication());
    }

    /**
     * Get the {@link ContextApplication} instance associated with the current thread or throw an exception if there is none.
     *
     * <p>
     * If the current thread is handling a Vaadin web request that is executing within an {@link ContextApplication} instance,
     * or is executing within {@link #invoke}, then this method will return the associated {@link ContextApplication}.
     * Otherwise, an exception is thrown.
     * </p>
     *
     * @return the {@link ContextApplication} associated with the current thread
     * @throws IllegalStateException if the current thread is not servicing a Vaadin web request
     *  or the current Vaadin {@link Application} is not an {@link ContextApplication}
     */
    public static ContextApplication get() {
        ContextApplication app = ContextApplication.currentApplication();
        if (app != null)
            return app;
        throw new IllegalStateException("no current application found");
    }

    /**
     * Get the {@link ContextApplication} instance associated with the current thread, cast to the desired type,
     * or throw an exception if there is none.
     *
     * @param type expected application type
     * @return the {@link ContextApplication} associated with the current thread
     * @throws IllegalStateException if the current {@link ContextApplication} is not found
     * @see #get()
     * @throws ClassCastException if the current application is not assignable to {@code type}
     */
    public static <A extends ContextApplication> A get(Class<A> type) {
        A app = ContextApplication.currentApplication(type);
        if (app != null)
            return app;
        throw new IllegalStateException("no current application found");
    }

    /**
     * Get the {@link HttpServletRequest} associated with the current thread.
     *
     * <p>
     * If the current thread is handling a Vaadin web request for an instance of this class,
     * this method will return the associated {@link HttpServletRequest}.
     * </p>
     *
     * @return the {@link HttpServletRequest} associated with the current thread, or {@code null} if the current thread
     *  is not servicing a Vaadin web request or the current Vaadin {@link Application} is not an instance of this class
     */
    public static HttpServletRequest currentRequest() {
        return ContextApplication.CURRENT_REQUEST.get();
    }

    /**
     * Get the {@link HttpServletResponse} associated with the current thread.
     *
     * <p>
     * If the current thread is handling a Vaadin web request for an instance of this class,
     * this method will return the associated {@link HttpServletResponse}.
     * </p>
     *
     * @return the {@link HttpServletResponse} associated with the current thread, or {@code null} if the current thread
     *  is not servicing a Vaadin web request or the current Vaadin {@link Application} is not an instance of this class
     */
    public static HttpServletResponse currentResponse() {
        return ContextApplication.CURRENT_RESPONSE.get();
    }

// Listener stuff

    /**
     * Close this instance.
     *
     * <p>
     * The implementation in {@link ContextApplication} first delegates to the superclass and then
     * notifies any registered {@link CloseListener}s.
     * </p>
     */
    @Override
    public void close() {

        // Invoke superclass
        super.close();

        // Notify listeners
        CloseEvent closeEvent = new CloseEvent(this);
        for (CloseListener closeListener : this.getCloseListeners()) {
            try {
                closeListener.applicationClosed(closeEvent);
            } catch (ThreadDeath t) {
                throw t;
            } catch (Throwable t) {
                this.log.error("exception thrown by CloseListener " + closeListener, t);
            }
        }

        // Shutdown ExecutorService
        this.shutdownExecutorService();
    }

    private void shutdownExecutorService() {

        // Already shutdown?
        if (this.executorService == null)
            return;

        // Shut it down; waiting up to 1 second to finish
        this.executorService.shutdown();
        boolean terminated = false;
        try {
            terminated = this.executorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ok, give up
        }

        // Log warnings if it didn't finish
        if (!terminated) {
            this.log.warn("forcibly terminating outstanding tasks for closed Vaadin application " + this);
            List<Runnable> list = this.executorService.shutdownNow();
            if (!list.isEmpty())
                this.log.warn(list.size() + " outstanding task(s) remain for closed Vaadin application " + this + ": " + list);
        }

        // Done
        this.executorService = null;
    }

    /**
     * Add a {@link CloseListener} to be notified when this instance is closed.
     */
    public void addListener(CloseListener listener) {
        synchronized (this.closeListeners) {
            this.closeListeners.add(listener);
        }
    }

    /**
     * Remove a {@link CloseListener}.
     */
    public void removeListener(CloseListener listener) {
        synchronized (this.closeListeners) {
            this.closeListeners.remove(listener);
        }
    }

    private HashSet<CloseListener> getCloseListeners() {
        synchronized (this.closeListeners) {
            return new HashSet<CloseListener>(this.closeListeners);
        }
    }

    /**
     * Implemented by listeners that wish to be notified when a {@link ContextApplication} is closed.
     *
     * @see ContextApplication#addListener
     */
    public interface CloseListener {

        /**
         * Notification upon {@link ContextApplication} closing.
         */
        void applicationClosed(CloseEvent closeEvent);
    }

    /**
     * Event delivered to listeners when a {@link ContextApplication} is closed.
     */
    public static class CloseEvent extends EventObject {

        public CloseEvent(ContextApplication application) {
            super(application);
        }

        /**
         * Get the {@link ContextApplication} that is being closed.
         */
        public ContextApplication getContextApplication() {
            return (ContextApplication)super.getSource();
        }
    }

    // Used by invokeLater()
    private class LaterRunnable implements Runnable {

        private final Runnable action;

        LaterRunnable(Runnable action) {
            this.action = action;
        }

        @Override
        public void run() {
            try {
                ContextApplication.this.invoke(this.action);
            } catch (ThreadDeath t) {
                throw t;
            } catch (Throwable e) {
                ContextApplication.this.log.error("exception thrown by invokeLater() action " + this.action, e);
            }
        }

        @Override
        public String toString() {
            return this.action.toString();
        }
    }
}

