
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.VaadinServlet;
import com.vaadin.server.VaadinServletService;

import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

/**
 * A {@link VaadinServlet} that manages an associated Spring
 * {@link org.springframework.web.context.ConfigurableWebApplicationContext} with each
 * {@link com.vaadin.server.VaadinServiceSession} (aka, "Vaadin application" in the old terminology).
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
 * The only function of this servlet is to create and register a {@link SpringServiceSession} as a listener on the
 * {@link com.vaadin.server.VaadinService} associated with this servlet. The {@link SpringServiceSession} in turn detects
 * the creation and destruction of Vaadin application instances (represented by {@link com.vaadin.server.VaadinServiceSession}
 * instances) and does the work of managing the associated Spring application context.
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
 *  Specify the name of a custom class extending {@link SpringServiceSession} and having the same constructor arguments.
 *  If omitted, {@link SpringServiceSession} is used.
 * </td>
 * </tr>
 * </table>
 * </div>
 * </p>
 *
 * @see SpringServiceSession
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
     * the name of an custom subclass of {@link SpringServiceSession}.
     * This parameter is optional.
     */
    public static final String LISTENER_CLASS_PARAMETER = "listenerClass";

    /**
     * Servlet initialization parameter (<code>{@value #APPLICATION_NAME_PARAMETER}</code>) used to specify
     * the name the application.
     * This parameter is optional.
     */
    public static final String APPLICATION_NAME_PARAMETER = "applicationName";

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
        Class<? extends SpringServiceSession> listenerClass = SpringServiceSession.class;
        if (listenerClassName != null) {
            try {
                listenerClass = Class.forName(listenerClassName, false, Thread.currentThread().getContextClassLoader())
                  .asSubclass(SpringServiceSession.class);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new ServletException("error finding class " + listenerClassName, e);
            }
        }

        // Create session listener
        SpringServiceSession sessionListener;
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
}

