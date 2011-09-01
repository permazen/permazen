
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.Application;
import com.vaadin.terminal.gwt.server.ApplicationServlet;
import com.vaadin.terminal.gwt.server.SessionExpiredException;

import java.io.IOException;
import java.net.MalformedURLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vaadin {@link ApplicationServlet} that provides access to the current request context via static methods.
 *
 * <p>
 * Specifically, this class makes the following available to the thread handling a Vaadin web request:
 * <ul>
 * <li>The current {@link Application} via {@link #currentApplication}</li>
 * <li>The current {@link HttpServletRequest} via {@link #currentRequest}</li>
 * <li>The current {@link HttpServletResponse} via {@link #currentResponse}</li>
 * </ul>
 * </p>
 * @since 1.0.122
 */
@SuppressWarnings("serial")
public class ContextApplicationServlet extends ApplicationServlet {

    private static final ThreadLocal<Application> CURRENT_APPLICATION = new ThreadLocal<Application>();
    private static final ThreadLocal<HttpServletRequest> CURRENT_REQUEST = new ThreadLocal<HttpServletRequest>();
    private static final ThreadLocal<HttpServletResponse> CURRENT_RESPONSE = new ThreadLocal<HttpServletResponse>();

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Creates a new {@link Application} instance.
     *
     * <p>
     * The implementation in {@link ContextApplicationServlet} delegates to {@link #createNewApplication}
     * to do the actual {@link Application} instance creation.
     * </p>
     */
    @Override
    protected final Application getNewApplication(HttpServletRequest request) throws ServletException {

        // Get application instance
        Application application = this.createNewApplication(request);

        // Save thread-local reference
        this.setCurrentApplication(request, application);

        // Done
        return application;
    }

    /**
     * Do the actual work of creating a new {@link Application} instance.
     *
     * <p>
     * The implementation in {@link ContextApplicationServlet} simply instantiates a new instance
     * of the application class returned by {@link #getApplicationClass}.
     * </p>
     */
    protected Application createNewApplication(HttpServletRequest request) throws ServletException {

        // Get application class
        Class<? extends Application> applicationClass;
        try {
            applicationClass = this.getApplicationClass();
        } catch (ClassNotFoundException e) {
            throw new ServletException("can't determine application class", e);
        }

        // Instantiate it
        try {
            return applicationClass.newInstance();
        } catch (Exception e) {
            throw new ServletException("can't instantiate " + applicationClass, e);
        }
    }

    @Override
    protected Application getExistingApplication(HttpServletRequest request, boolean allowSessionCreation)
      throws MalformedURLException, SessionExpiredException {

        // Delegate to superclass
        Application application = super.getExistingApplication(request, allowSessionCreation);

        // Save thread-local reference
        this.setCurrentApplication(request, application);

        // Done
        return application;
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // Sanity check
        if (ContextApplicationServlet.CURRENT_REQUEST.get() != null
          || ContextApplicationServlet.CURRENT_RESPONSE.get() != null
          || ContextApplicationServlet.CURRENT_APPLICATION.get() != null)
            throw new ServletException("invalid re-entrant execution of " + this.getClass().getName() + ".service()");

        // Save thread-local references to request and response
        ContextApplicationServlet.CURRENT_REQUEST.set(request);
        ContextApplicationServlet.CURRENT_RESPONSE.set(response);

        // Defer to superclass
        try {
            super.service(request, response);
        } finally {
            ContextApplicationServlet.CURRENT_REQUEST.remove();
            ContextApplicationServlet.CURRENT_RESPONSE.remove();
            ContextApplicationServlet.CURRENT_APPLICATION.remove();
        }
    }

    private void setCurrentApplication(HttpServletRequest request, Application application) {
        if (ContextApplicationServlet.CURRENT_REQUEST.get() != request) {
            this.log.warn("unexpected state: the current HTTP request (" + ContextApplicationServlet.CURRENT_REQUEST.get()
              + ") is not the same as the provided HTTP request (" + request + ")");
            return;
        }
        ContextApplicationServlet.CURRENT_APPLICATION.set(application);
    }

    /**
     * Get the {@link Application} associated with the current thread.
     *
     * <p>
     * If the current thread is handling a Vaadin web request, this method will return the associated {@link Application}.
     * </p>
     *
     * @return the {@link Application} associated with the current thread, or {@code null} if the current thread
     *  is not servicing a Vaadin web request or the current Vaadin {@link Application} was not created by this servlet.
     */
    public static Application currentApplication() {
        return ContextApplicationServlet.CURRENT_APPLICATION.get();
    }

    /**
     * Get the {@link HttpServletRequest} associated with the current thread.
     *
     * <p>
     * If the current thread is handling a Vaadin web request, this method will return the associated {@link HttpServletRequest}.
     * </p>
     *
     * @return the {@link HttpServletRequest} associated with the current thread, or {@code null} if the current thread
     *  is not servicing a Vaadin web request or the current Vaadin {@link Application} was not created by this servlet.
     */
    public static HttpServletRequest currentRequest() {
        return ContextApplicationServlet.CURRENT_REQUEST.get();
    }

    /**
     * Get the {@link HttpServletResponse} associated with the current thread.
     *
     * <p>
     * If the current thread is handling a Vaadin web request, this method will return the associated {@link HttpServletResponse}.
     * </p>
     *
     * @return the {@link HttpServletResponse} associated with the current thread, or {@code null} if the current thread
     *  is not servicing a Vaadin web request or the current Vaadin {@link Application} was not created by this servlet.
     */
    public static HttpServletResponse currentResponse() {
        return ContextApplicationServlet.CURRENT_RESPONSE.get();
    }
}

