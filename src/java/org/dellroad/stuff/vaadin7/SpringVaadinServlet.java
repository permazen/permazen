
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.DeploymentConfiguration;
import com.vaadin.server.ServiceException;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServlet;
import com.vaadin.server.VaadinServletService;
import com.vaadin.server.VaadinSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.WeakHashMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

/**
 * A {@link VaadinServlet} that associates and manages a Spring
 * {@link org.springframework.web.context.ConfigurableWebApplicationContext} with each
 * {@link com.vaadin.server.VaadinSession} (aka, "Vaadin application" in the old terminology).
 *
 * <p>
 * The {@code vaadinContextConfigLocation} servlet parameter may be used to specify the Spring XML config
 * file location(s). For example:
 *
 * <blockquote><pre>
 * &lt;servlet&gt;
 *     &lt;servlet-name&gt;My Vaadin App&lt;/servlet-name&gt;
 *     &lt;servlet-class&gt;org.dellroad.stuff.vaadin7.SpringVaadinServlet&lt;/servlet-class&gt;
 *     &lt;init-param&gt;
 *         &lt;param-name&gt;UI&lt;/param-name&gt;
 *         &lt;param-value&gt;com.example.MyApplicationUI&lt;/param-value&gt;
 *     &lt;/init-param&gt;
 *     &lt;init-param&gt;
 *         &lt;param-name&gt;configLocation&lt;/param-name&gt;
 *         &lt;param-value&gt;classpath:com/example/MyApplicationContext.xml&lt;/param-value&gt;
 *     &lt;/init-param&gt;
 * &lt;/servlet&gt;
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * The main function of this servlet is to create and register a {@link SpringVaadinSessionListener} as a listener on the
 * {@link com.vaadin.server.VaadinService} associated with this servlet. The {@link SpringVaadinSessionListener} in turn detects
 * the creation and destruction of Vaadin application instances (represented by {@link com.vaadin.server.VaadinSession}
 * instances) and does the work of managing the associated Spring application contexts.
 * </p>
 *
 * <p>
 * Use of this servlet in place of the standard Vaadin servlet is required for the {@link VaadinConfigurable @VaadinConfigurable}
 * annotation to work.
 * </p>
 *
 * <p>
 * Supported URL parameters:
 * <div style="margin-left: 20px;">
 * <table border="1" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#ccffcc">
 *  <th align="left">Parameter Name</th>
 *  <th align="left">Required?</th>
 *  <th align="left">Description</th>
 * </tr>
 * <tr>
 * <td>{@code applicationName}</td>
 * <td align="center">No</td>
 * <td>
 *  Vaadin application name. Used for logging purposes and as the name of the XML application context file
 *  when {@code configLocation} is not specified. If this parameter is not specified, the
 *  name of the servlet is used.
 * </td>
 * </tr>
 * <tr>
 * <td>{@code configLocation}</td>
 * <td align="center">No</td>
 * <td>
 *  Location of Spring application context XML file(s). Multiple locations are separated by whitespace.
 *  If omitted, {@code /WEB-INF/ServletName.xml} is used, where {@code ServletName} is the name of the Vaadin
 *  application (see {@code applicationName}).
 * </td>
 * </tr>
 * <tr>
 * <td>{@code listenerClass}</td>
 * <td align="center">No</td>
 * <td>
 *  Specify the name of a custom class extending {@link SpringVaadinSessionListener} and having the same constructor arguments.
 *  If omitted, {@link SpringVaadinSessionListener} is used.
 * </td>
 * </tr>
 * <tr>
 * <td>{@code sessionTracking}</td>
 * <td align="center">No</td>
 * <td>
 *  Boolean value that configures whether the {@link SpringVaadinSessionListener} should track Vaadin sessions; default
 *  {@code false}. If set to {@code true}, then {@link #getSessions} can be used to access all active sessions.
 *  Session tracking should not be used unless sessions are normally kept in memory; e.g., don't use session tracking
 *  when sessions are being serialized and persisted. See also {@link VaadinSessionContainer}.
 * </td>
 * </tr>
 * <tr>
 * <td>{@code maxSessions}</td>
 * <td align="center">No</td>
 * <td>
 *  Configures a limit on the number of simultaneous Vaadin sessions that may exist at one time. Going over this
 *  limit will result in a {@link com.vaadin.server.ServiceException} being thrown. A zero or negative number
 *  means there is no limit (this is the default). Ignored unless {@value #SESSION_TRACKING_PARAMETER} is set to {@code true}.
 * </td>
 * </tr>
 * </table>
 * </div>
 * </p>
 *
 * @see SpringVaadinSessionListener
 * @see VaadinConfigurable
 * @see VaadinApplication
 */
@SuppressWarnings("serial")
public class SpringVaadinServlet extends VaadinServlet {

    /**
     * Servlet initialization parameter (<code>{@value #CONFIG_LOCATION_PARAMETER}</code>) used to specify
     * the location(s) of the Spring application context XML file(s). Multiple XML files may be separated by whitespace.
     * This parameter is optional.
     */
    public static final String CONFIG_LOCATION_PARAMETER = "configLocation";

    /**
     * Servlet initialization parameter (<code>{@value #LISTENER_CLASS_PARAMETER}</code>) used to specify
     * the name of an custom subclass of {@link SpringVaadinSessionListener}.
     * This parameter is optional.
     */
    public static final String LISTENER_CLASS_PARAMETER = "listenerClass";

    /**
     * Servlet initialization parameter (<code>{@value #APPLICATION_NAME_PARAMETER}</code>) used to specify
     * the name the application.
     * This parameter is optional.
     */
    public static final String APPLICATION_NAME_PARAMETER = "applicationName";

    /**
     * Servlet initialization parameter (<code>{@value #SESSION_TRACKING_PARAMETER}</code>) that enables
     * tracking of all Vaadin session.
     * This parameter is optional, and defaults to <code>false</code>.
     */
    public static final String SESSION_TRACKING_PARAMETER = "sessionTracking";

    /**
     * Servlet initialization parameter (<code>{@value #MAX_SESSIONS_PARAMETER}</code>) that configures the
     * maximum number of simultaneous Vaadin sessions. Requires {@link #SESSION_TRACKING_PARAMETER} to be set to {@code true}.
     * This parameter is optional, and defaults to zero, which means no limit.
     */
    public static final String MAX_SESSIONS_PARAMETER = "maxSessions";

    // We use weak references to avoid leaks caused by exceptions in SessionInitListeners; see http://dev.vaadin.com/ticket/12915
    private final WeakHashMap<VaadinSession, Void> liveSessions = new WeakHashMap<VaadinSession, Void>();

    private String servletName;

    @Override
    public void init(ServletConfig config) throws ServletException {
        this.servletName = config.getServletName();
        if (this.servletName == null)
            throw new IllegalArgumentException("null servlet name");
        super.init(config);
    }

    @Override
    protected void servletInitialized() throws ServletException {

        // Sanity check
        if (this.servletName == null)
            throw new IllegalArgumentException("servlet not initialized");

        // Defer to superclass
        super.servletInitialized();

        // Get params
        final VaadinServletService servletService = this.getService();
        final Properties params = servletService.getDeploymentConfiguration().getInitParameters();
        final String contextLocation = params.getProperty(CONFIG_LOCATION_PARAMETER);
        final String listenerClassName = params.getProperty(LISTENER_CLASS_PARAMETER);
        String applicationName = params.getProperty(APPLICATION_NAME_PARAMETER);
        if (applicationName == null)
            applicationName = this.servletName;

        // Detect listener class to use
        Class<? extends SpringVaadinSessionListener> listenerClass = SpringVaadinSessionListener.class;
        if (listenerClassName != null) {
            try {
                listenerClass = Class.forName(listenerClassName, false, Thread.currentThread().getContextClassLoader())
                  .asSubclass(SpringVaadinSessionListener.class);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new ServletException("error finding class " + listenerClassName, e);
            }
        }

        // Create session listener
        SpringVaadinSessionListener sessionListener;
        try {
            sessionListener = listenerClass.getConstructor(String.class, String.class)
              .newInstance(applicationName, contextLocation);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ServletException("error instantiating " + listenerClass, e);
        }

        // Register session listener
        servletService.addSessionInitListener(sessionListener);
        servletService.addSessionDestroyListener(sessionListener);
    }

    @Override
    protected VaadinServletService createServletService(DeploymentConfiguration deploymentConfiguration) throws ServiceException {

        // Get session tracking parameters
        final Properties params = deploymentConfiguration.getInitParameters();
        final boolean sessionTracking = Boolean.valueOf(params.getProperty(SESSION_TRACKING_PARAMETER));
        int maxSessionsParam = 0;
        try {
            maxSessionsParam = Integer.parseInt(params.getProperty(MAX_SESSIONS_PARAMETER));
        } catch (Exception e) {
            // ignore
        }
        final int maxSessions = maxSessionsParam;

        // If not tracking sessions, do the normal thing
        if (!sessionTracking)
            return super.createServletService(deploymentConfiguration);

        // Return a VaadinServletService that tracks sessions
        final VaadinServletService service = new VaadinServletService(this, deploymentConfiguration) {

            @Override
            protected VaadinSession createVaadinSession(VaadinRequest request) throws ServiceException {
                if (maxSessions > 0 && SpringVaadinServlet.this.liveSessions.size() >= maxSessions)
                    throw new ServiceException("The maximum number of active sessions has been reached");
                final VaadinSession session = super.createVaadinSession(request);
                SpringVaadinServlet.this.liveSessions.put(session, null);
                return session;
            }

            @Override
            public void fireSessionDestroy(VaadinSession session) {
                SpringVaadinServlet.this.liveSessions.remove(session);
                super.fireSessionDestroy(session);
            }
        };
        service.init();
        return service;
    }

    /**
     * Get all live {@link VaadinSession}s associated with this instance.
     *
     * @return live tracked sessions, or an empty collection if session tracking is not enabled
     * @see VaadinSessionContainer
     */
    public synchronized List<VaadinSession> getSessions() {
        return new ArrayList<VaadinSession>(this.liveSessions.keySet());
    }

    /**
     * Get the {@link SpringVaadinServlet} that is associated with the {@link VaadinSession}
     * that is associated with the current thread.
     *
     * @throws IllegalStateException if there is no {@link VaadinServlet} associated with the current thread
     * @throws IllegalStateException if the {@link VaadinServlet} associated with the current thread is not a
     *  {@link SpringVaadinServlet}
     */
    public static SpringVaadinServlet getCurrent() {
        final VaadinServlet vaadinServlet = VaadinServlet.getCurrent();
        if (vaadinServlet == null) {
            throw new IllegalStateException("there is no VaadinServlet associated with the current thread;"
              + " are we executing within a Vaadin HTTP request?");
        }
        if (!(vaadinServlet instanceof SpringVaadinServlet))
            throw new IllegalStateException("the VaadinServlet associated with the current thread is not a SpringVaadinServlet");
        return (SpringVaadinServlet)vaadinServlet;
    }
}

