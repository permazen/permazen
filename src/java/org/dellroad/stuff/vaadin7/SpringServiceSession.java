
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.SessionDestroyEvent;
import com.vaadin.server.SessionDestroyListener;
import com.vaadin.server.SessionInitEvent;
import com.vaadin.server.SessionInitListener;
import com.vaadin.server.VaadinPortletRequest;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServiceSession;
import com.vaadin.server.VaadinServletRequest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.context.support.XmlWebApplicationContext;

/**
 * Manages an associated Spring {@link WebApplicationContext} with each {@link VaadinServiceSession} (aka, "Vaadin application").
 * Typically created implicitly via {@link SpringVaadinServlet}.
 *
 * <h3>Overview</h3>
 *
 * <p>
 * In Vaadin 7, the {@link com.vaadin.server.VaadinServiceSession} object holds the state associated with each client browser
 * connection to a Vaadin servlet. For consistency with older versions of Vaadin, we'll call this a "Vaadin application" instance.
 * This class gives each Vaadin such "Vaadin application" instance its own Spring application context, and all such
 * application contexts share the same parent context, which is the one associated with the overal servlet web context
 * (i.e., the one created by Spring's {@link org.springframework.web.context.ContextLoaderListener ContextLoaderListener}).
 * This setup is analogous to how Spring's {@link org.springframework.web.servlet.DispatcherServlet DispatcherServlet}
 * creates per-servlet application contexts that are children of the overall servlet web context.
 * </p>
 *
 * <p>
 * This class is implemented as a {@link SessionInitListener} and {@link SessionDestroyListener} on the servlet's
 * {@link com.vaadin.server.VaadinService} object. In turn, the Spring context is created when a new Vaadin application
 * instance is initialized, and destroyed when it is closed. To use this class, use the {@link SpringVaadinServlet}
 * in place of the usual {@link com.vaadin.server.VaadinServlet} in {@code web.xml}.
 * </p>
 *
 * <h3>Accessing the Spring Context</h3>
 *
 * <p>
 * The {@link ConfigurableWebApplicationContext} itself is {@linkplain VaadinServiceSession#setAttribute stored as an attribute}
 * of the {@link VaadinServiceSession} under the key {@link #APPLICATION_CONTEXT_ATTRIBUTE_KEY}. The
 * {@link #getApplicationContext() getApplicationContext()} method provides convenient access from anywhere.
 * </p>
 *
 * <h3>Exposing the Vaadin Application</h3>
 *
 * <p>
 * The {@link VaadinServiceSession} instance representing the "Vaadin application" can be exposed in the associated Spring
 * application context and therefore made available for autowiring, etc. Simply add a bean definition that invokes
 * {@link VaadinUtil#getCurrentSession}:
 * <blockquote><pre>
 *  &lt;bean id="vaadinServiceSession" class="org.dellroad.stuff.vaadin7.VaadinUtil" factory-method="getCurrentSession"/&gt;
 * </pre></blockquote>
 * This bean can then be autowired into application-specific "backend" beans, allowing them to use e.g.
 * {@link VaadinUtil#invoke VaadinUtil.invoke()}, which performs the locking necessary to avoid race conditions.
 * But see also {@link VaadinApplication} for a convenience class that makes this process a little cleaner.
 *
 * <h3><code>@VaadinConfigurable</code> Beans</h3>
 *
 * <p>
 * It is also possible to configure beans outside of this application context using AOP, so that any invocation of
 * {@code new FooBar()}, where the class {@code FooBar} is marked {@link VaadinConfigurable @VaadinConfigurable},
 * will automagically cause the new {@code FooBar} object to be configured by the application context associated with
 * the currently running Vaadin application. In effect, this does for Vaadin application beans what Spring's
 * {@link org.springframework.beans.factory.annotation.Configurable @Configurable} does for regular beans.
 * </p>
 *
 * <p>
 * Note however that Spring {@linkplain org.springframework.beans.factory.DisposableBean#destroy destroy methods}
 * will not be invoked on application close for these beans, since their lifecycle is controlled outside of the
 * Spring application context (this is also the case with
 * {@link org.springframework.beans.factory.annotation.Configurable @Configurable} beans). Instead, these beans
 * can themselves register as a {@link SessionDestroyListener} for shutdown notification; see
 * {@link VaadinUtil#addSessionDestroyListener VaadinUtil.addSessionDestroyListener()} for a memory-leak free
 * method for doing this.
 * </p>
 *
 * @see VaadinConfigurable
 * @see SpringVaadinServlet
 * @see VaadinApplication
 * @see com.vaadin.server.VaadinService
 * @see com.vaadin.server.VaadinServiceSession
 */
@SuppressWarnings("serial")
public class SpringServiceSession implements SessionInitListener, SessionDestroyListener {

    /**
     * The {@link VaadinServiceSession} attribute key under which the Spring {@link ConfigurableWebApplicationContext}
     * is stored.
     */
    public static final String APPLICATION_CONTEXT_ATTRIBUTE_KEY = "springServiceSessionApplicationContext";

    private static final AtomicLong UNIQUE_INDEX = new AtomicLong();

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final String applicationName;
    private final String configLocation;

    /**
     * Constructor.
     *
     * @param applicationName Vaadin application name
     * @param configLocation XML application context config file location(s), or null for the default
     *  {@code /WEB-INF/ServletName.xml}, where {@code ServletName} is the value of {@code applicationName}
     * @throws IllegalArgumentException if {@code applicationName} is null
     */
    public SpringServiceSession(String applicationName, String configLocation) {
        if (applicationName == null)
            throw new IllegalArgumentException("null applicationName");
        this.applicationName = applicationName;
        this.configLocation = configLocation;
    }

    /**
     * Get the name of this Vaadin application.
     */
    public String getApplicationName() {
        return this.applicationName;
    }

    /**
     * Get the Spring application context associated with the given {@link VaadinServiceSession}.
     *
     * @return Spring application context, or null if none is found
     * @throws IllegalArgumentException if {@code session} is null
     */
    public static ConfigurableWebApplicationContext getApplicationContext(VaadinServiceSession session) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        return (ConfigurableWebApplicationContext)session.getAttribute(APPLICATION_CONTEXT_ATTRIBUTE_KEY);
    }

    /**
     * Get the Spring application context associated with the current thread's {@link VaadinServiceSession}.
     *
     * @return Spring application context, never null
     * @throws IllegalStateException if there is no {@link VaadinServiceSession} associated with the current thread
     * @throws IllegalStateException if there is no Spring application context associated with the {@link VaadinServiceSession}
     */
    public static ConfigurableWebApplicationContext getApplicationContext() {

        // Get current session
        VaadinServiceSession session = VaadinUtil.getCurrentSession();

        // Get the associated application context
        ConfigurableWebApplicationContext context = SpringServiceSession.getApplicationContext(session);
        if (context == null) {
            throw new IllegalStateException("there is no Spring application context associated with the current"
              + " VaadinServiceSession; are you using SpringVaadinServlet instead of VaadinServlet?");
        }

        // Done
        return context;
    }

// SessionInitListener

    @Override
    public void sessionInit(final SessionInitEvent event) {
        VaadinUtil.invoke(event.getSession(), new Runnable() {
            @Override
            public void run() {
                SpringServiceSession.this.loadContext(event.getSession(), event.getRequest());
            }
        });
    }

// Context loading

    /**
     * Load the Spring application context.
     *
     * <p>
     * This method expects that {@code session} is already {@linkplain VaadinServiceSession#getLock locked}.
     *
     * @param session the Vaadin application session
     * @param request the triggering request
     */
    protected void loadContext(VaadinServiceSession session, VaadinRequest request) {

        // Sanity check
        if (session == null)
            throw new IllegalStateException("null session");
        if (request == null)
            throw new IllegalStateException("null request");
        if (this.getApplicationContext(session) != null)
            throw new IllegalStateException("context already loaded");

        // Logging
        this.log.info("creating new application context for Vaadin application [" + this.getApplicationName()
          + "] in session " + session);

        // Find the servlet context parent application context
        ServletContext servletContext = null;
        WebApplicationContext parent = null;
        if (request instanceof VaadinServletRequest) {

            // Find servlet context
            HttpServletRequest httpRequest = ((VaadinServletRequest)request).getHttpServletRequest();
            try {
                // getServletContext() is a servlet AIP 3.0 method, so don't freak out if it's not there
                servletContext = (ServletContext)HttpServletRequest.class.getMethod("getServletContext").invoke(httpRequest);
            } catch (Exception e) {
                servletContext = ContextLoader.getCurrentWebApplicationContext().getServletContext();
            }

            // Find associated application context; it will become our parent context
            parent = WebApplicationContextUtils.getWebApplicationContext(servletContext);
        } else if (request instanceof VaadinPortletRequest) {
            // TODO
        }

        // Create and configure a new application context for this Application instance
        XmlWebApplicationContext context = new XmlWebApplicationContext();
        context.setId(ConfigurableWebApplicationContext.APPLICATION_CONTEXT_ID_PREFIX
          + (servletContext != null ? servletContext.getContextPath() + "/" : "")
          + this.getApplicationName() + "-" + UNIQUE_INDEX.incrementAndGet());
        if (parent != null)
            context.setParent(parent);
        if (servletContext != null)
            context.setServletContext(servletContext);
        //context.setServletConfig(??);
        context.setNamespace(this.getApplicationName());

        // Set explicit config location(s), if any
        if (this.configLocation != null)
            context.setConfigLocation(this.configLocation);

        // Associate context with the current session
        boolean success = false;
        session.setAttribute(APPLICATION_CONTEXT_ATTRIBUTE_KEY, context);

        // Refresh context
        try {
            context.refresh();
            success = true;
        } finally {
            if (!success)
                session.setAttribute(APPLICATION_CONTEXT_ATTRIBUTE_KEY, null);
        }
    }

// SessionDestroyListener

    @Override
    public void sessionDestroy(SessionDestroyEvent event) {
        final VaadinServiceSession session = event.getSession();
        final ConfigurableWebApplicationContext context = this.getApplicationContext(session);
        if (context == null) {
            this.log.error(this.getClass().getSimpleName() + ".sessionDestroy() invoked but no application context found");
            return;
        }
        session.setAttribute(APPLICATION_CONTEXT_ATTRIBUTE_KEY, null);
        VaadinUtil.invoke(session, new Runnable() {
            @Override
            public void run() {
                SpringServiceSession.this.log.info("closing Vaadin application ["
                  + SpringServiceSession.this.getApplicationName() + "] application context: " + context);
                context.close();
            }
        });
    }

// Serialization

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        final VaadinServiceSession session = VaadinUtil.getCurrentSession();
        final VaadinRequest request = VaadinUtil.getCurrentRequest();
        VaadinUtil.invoke(session, new Runnable() {
            @Override
            public void run() {
                SpringServiceSession.this.loadContext(session, request);
            }
        });
    }
}

