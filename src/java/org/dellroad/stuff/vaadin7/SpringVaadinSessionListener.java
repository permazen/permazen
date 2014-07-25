
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.ServiceException;
import com.vaadin.server.SessionDestroyEvent;
import com.vaadin.server.SessionDestroyListener;
import com.vaadin.server.SessionInitEvent;
import com.vaadin.server.SessionInitListener;
import com.vaadin.server.VaadinPortletRequest;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServletRequest;
import com.vaadin.server.VaadinSession;
import com.vaadin.server.WrappedHttpSession;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.UUID;
import java.util.WeakHashMap;

import javax.servlet.ServletContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.wiring.BeanConfigurerSupport;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.context.support.XmlWebApplicationContext;

/**
 * Manages an associated Spring {@link WebApplicationContext} with each {@link VaadinSession} (aka, "Vaadin application").
 * Typically created implicitly via {@link SpringVaadinServlet}.
 *
 * <h3>Overview</h3>
 *
 * <p>
 * In Vaadin 7, the {@link com.vaadin.server.VaadinSession} object holds the state associated with each client browser
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
 * The {@link #getApplicationContext() getApplicationContext()} method provides access to the application context.
 * Alternately, use {@link VaadinConfigurable @VaadinConfigurable} (see below) and implement
 * {@link org.springframework.context.ApplicationContextAware}, etc.
 * Invoking {@link #configureBean configureBean()} at any time will configure a bean manually.
 * </p>
 *
 * <h3>Exposing the Vaadin Session</h3>
 *
 * <p>
 * The {@link VaadinSession} instance representing the "Vaadin application" can be exposed in the associated Spring
 * application context and therefore made available for autowiring, etc. Simply add a bean definition that invokes
 * {@link VaadinUtil#getCurrentSession}:
 * <blockquote><pre>
 *  &lt;bean id="vaadinSession" class="org.dellroad.stuff.vaadin7.VaadinUtil" factory-method="getCurrentSession"/&gt;
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
 * {@link org.springframework.beans.factory.annotation.Configurable @Configurable} does for regular servlet context-wide beans.
 * </p>
 *
 * <p>
 * Note however that Spring {@linkplain org.springframework.beans.factory.DisposableBean#destroy destroy methods}
 * will not be invoked on application close for these beans, since their lifecycle is controlled outside of the
 * Spring application context (this is also the case with
 * {@link org.springframework.beans.factory.annotation.Configurable @Configurable} beans). Instead, these beans
 * can themselves register as a {@link SessionDestroyListener} for shutdown notification; but see
 * {@link VaadinUtil#addSessionDestroyListener VaadinUtil.addSessionDestroyListener()} for a memory-leak free
 * method for doing this.
 * </p>
 *
 * <h3>Serialization and Clustering</h3>
 *
 * <p>
 * Instances are serializable; on deserialization the {@link ConfigurableWebApplicationContext} associated with the
 * {@link VaadinSession} is {@linkplain ConfigurableWebApplicationContext#refresh refreshed}; therefore, the
 * {@link ConfigurableWebApplicationContext} is not itself stored in the HTTP session by this class (as is typical with Spring).
 * </p>
 *
 * <p>
 * However, any session-scope beans should work as expected.
 * So while this class associates an application context with each {@link VaadinSession}, when sessions are shared across
 * multiple servers in a clustered environment, there will actually be a separate application contexts per server.
 * Therefore, beans that are truly "session wide" should be declared {@code scope="session"}.
 * </p>
 *
 * @see VaadinConfigurable
 * @see SpringVaadinServlet
 * @see VaadinApplication
 * @see com.vaadin.server.VaadinService
 * @see com.vaadin.server.VaadinSession
 */
public class SpringVaadinSessionListener implements SessionInitListener, SessionDestroyListener {

    private static final long serialVersionUID = -2107311484324869198L;
    private static final WeakHashMap<VaadinSession, ConfigurableWebApplicationContext> CONTEXT_MAP = new WeakHashMap<>();

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final UUID uuid = UUID.randomUUID();            // ensures a unique ID for the associated context across a cluster
    private final String applicationName;
    private final String configLocation;

// Constructor

    /**
     * Constructor.
     *
     * @param applicationName Vaadin application name
     * @param configLocation XML application context config file location(s), or null for the default
     *  {@code /WEB-INF/ServletName.xml}, where {@code ServletName} is the value of {@code applicationName}
     * @throws IllegalArgumentException if {@code applicationName} is null
     */
    public SpringVaadinSessionListener(String applicationName, String configLocation) {
        if (applicationName == null)
            throw new IllegalArgumentException("null applicationName");
        this.applicationName = applicationName;
        this.configLocation = configLocation;
    }

// Public API

    /**
     * Get the name of this Vaadin application.
     */
    public String getApplicationName() {
        return this.applicationName;
    }

    /**
     * Get the Spring application context associated with the given {@link VaadinSession}.
     *
     * @return Spring application context, or null if none is found
     * @throws IllegalArgumentException if {@code session} is null
     */
    public static ConfigurableWebApplicationContext getApplicationContext(VaadinSession session) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        return CONTEXT_MAP.get(session);
    }

    /**
     * Get the Spring application context associated with the current thread's {@link VaadinSession}.
     *
     * @return Spring application context, never null
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     * @throws IllegalStateException if there is no Spring application context associated with the {@link VaadinSession}
     */
    public static ConfigurableWebApplicationContext getApplicationContext() {

        // Get current session
        VaadinSession session = VaadinUtil.getCurrentSession();

        // Get the associated application context
        ConfigurableWebApplicationContext context = SpringVaadinSessionListener.getApplicationContext(session);
        if (context == null) {
            throw new IllegalStateException("there is no Spring application context associated with the current"
              + " VaadinSession; are you using SpringVaadinServlet instead of VaadinServlet?");
        }

        // Done
        return context;
    }

    /**
     * Configure the given bean using the Spring application context associated with the current thread's {@link VaadinSession}.
     *
     * @param bean Java bean to configure
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     * @throws IllegalStateException if there is no Spring application context associated with the {@link VaadinSession}
     */
    public static void configureBean(Object bean) {
        final BeanConfigurerSupport beanConfigurerSupport = new BeanConfigurerSupport();
        beanConfigurerSupport.setBeanFactory(SpringVaadinSessionListener.getApplicationContext().getBeanFactory());
        beanConfigurerSupport.afterPropertiesSet();
        beanConfigurerSupport.configureBean(bean);
        beanConfigurerSupport.destroy();
    }

// SessionInitListener

    @Override
    public void sessionInit(final SessionInitEvent event) throws ServiceException {
        VaadinUtil.invoke(event.getSession(), new Runnable() {
            @Override
            public void run() {
                SpringVaadinSessionListener.this.loadContext(event.getSession(), event.getRequest());
            }
        });
    }

// Context loading

    /**
     * Load the Spring application context.
     *
     * <p>
     * This method expects that {@code session} is already {@linkplain VaadinSession#getLockInstance locked}.
     *
     * @param session the Vaadin application session
     * @param request the triggering request
     */
    protected void loadContext(VaadinSession session, VaadinRequest request) {

        // Sanity check
        if (session == null)
            throw new IllegalStateException("null session");
        VaadinUtil.assertSession(session);
        if (request == null)
            throw new IllegalStateException("null request");
        if (SpringVaadinSessionListener.getApplicationContext(session) != null)
            throw new IllegalStateException("context already loaded for session " + session);

        // Logging
        this.log.info("creating new application context for Vaadin application [" + this.getApplicationName()
          + "] in session " + session);

        // Find the servlet context parent application context
        String contextPath = "/";
        ServletContext servletContext = null;
        WebApplicationContext parent = null;
        if (request instanceof VaadinServletRequest) {
            contextPath = ((VaadinServletRequest)request).getHttpServletRequest().getContextPath() + "/";
            servletContext = ((WrappedHttpSession)session.getSession()).getHttpSession().getServletContext();
            parent = WebApplicationContextUtils.getWebApplicationContext(servletContext);
        } else if (request instanceof VaadinPortletRequest)
            this.log.warn("portlets are not supported yet");
        else
            this.log.warn("unsupported VaadinRequest instance: " + request);

        // Create and configure a new application context for this Application instance
        final XmlWebApplicationContext context = new XmlWebApplicationContext();
        context.setId(ConfigurableWebApplicationContext.APPLICATION_CONTEXT_ID_PREFIX
          + contextPath + this.getApplicationName() + "-" + this.uuid);
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
        CONTEXT_MAP.put(session, context);

        // Refresh context
        try {
            context.refresh();
            success = true;
        } finally {
            if (!success)
                CONTEXT_MAP.remove(session);
        }
    }

// SessionDestroyListener

    @Override
    public void sessionDestroy(SessionDestroyEvent event) {
        final VaadinSession session = event.getSession();
        final ConfigurableWebApplicationContext context = SpringVaadinSessionListener.getApplicationContext(session);
        if (context == null) {
            this.log.info(this.getClass().getSimpleName() + ".sessionDestroy() invoked but no application context found"
              + " for Vaadin application [" + SpringVaadinSessionListener.this.getApplicationName() + "]");
            return;
        }
        CONTEXT_MAP.remove(session);
        VaadinUtil.invoke(session, new Runnable() {
            @Override
            public void run() {
                SpringVaadinSessionListener.this.log.info("closing Vaadin application ["
                  + SpringVaadinSessionListener.this.getApplicationName() + "] application context: " + context);
                context.close();
            }
        });
    }

// Serialization

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        final VaadinSession session = VaadinUtil.getCurrentSession();
        final VaadinRequest request = VaadinUtil.getCurrentRequest();
        VaadinUtil.invoke(session, new Runnable() {
            @Override
            public void run() {
                SpringVaadinSessionListener.this.loadContext(session, request);
            }
        });
    }
}

