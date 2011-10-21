
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import javax.servlet.ServletContext;

import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.SourceFilteringListener;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.context.support.XmlWebApplicationContext;

/**
 * {@link ContextApplication} implementation that loads and initializes an associated Spring {@link WebApplicationContext}
 * on application startup.
 *
 * <p>
 * This setup works analagously to the way Spring's {@link import org.springframework.web.servlet.DispatcherServlet
 * DispatcherServlet} creates a per-servlet application context whose parent context is the context associated with
 * the overal servlet context. In this case, each new {@link SpringContextApplication} instance results in a new
 * application context being created. When an {@link SpringContextApplication} instance is closed, the corresponding
 * Spring application context is also closed.
 * </p>
 *
 * <p>
 * By default, each instance of this class is itself autowired by the associated application context; this behavior
 * can be disabled by overriding {@link #isAutowire} to return {@code false}.
 * </p>
 *
 * <p>
 * For the application subclass {@code com.example.FooApplication}, this class will find and load an XML file named
 * {@code FooApplication.xml} to create the new Spring application context (this naming scheme {@linkplain #getApplicationName
 * can be overriden}).
 * </p>
 *
 * <p>
 * Note: Requires Servlet 3.0.
 * </p>
 */
@SuppressWarnings("serial")
public abstract class SpringContextApplication extends ContextApplication {

    private ConfigurableWebApplicationContext context;

    /**
     * Get this instance's associated application context.
     */
    public ConfigurableWebApplicationContext getApplicationContext() {
        return this.context;
    }

    /**
     * Initializes the associated {@link WebApplicationContext}.
     */
    protected final void initApplication() {

        // Logging
        this.log.info("creating new application context for Vaadin application " + this.getApplicationName());

        // Find the application context associated with the servlet; it will be the parent
        ServletContext servletContext = ContextApplication.currentRequest().getServletContext();
        WebApplicationContext parent = WebApplicationContextUtils.getWebApplicationContext(servletContext);

        // Create new application context for this Application instance
        this.context = BeanUtils.instantiateClass(XmlWebApplicationContext.class);

        // Configure application context
        context.setId(ConfigurableWebApplicationContext.APPLICATION_CONTEXT_ID_PREFIX
          + servletContext.getContextPath() + "/" + this.getApplicationName());
        context.setParent(parent);
        context.setServletContext(servletContext);
        //context.setServletConfig(??);
        context.setNamespace(this.getApplicationName());
        context.addApplicationListener(new SourceFilteringListener(context, new ApplicationListener<ContextRefreshedEvent>() {
            @Override
            public void onApplicationEvent(ContextRefreshedEvent event) {
                SpringContextApplication.this.onRefresh(event.getApplicationContext());
            }
        }));
        this.postProcessWebApplicationContext(context);

        // Refresh context
        context.refresh();

        // Get notified of application shutdown so we can shut down the context as well
        this.addListener(new CloseListener() {
            @Override
            public void applicationClosed(CloseEvent closeEvent) {
                SpringContextApplication.this.log.info("closing application context associated with Vaadin application "
                  + SpringContextApplication.this.getApplicationName());
                SpringContextApplication.this.context.close();
            }
        });

        // Autowire this bean from the context if desired
        if (this.isAutowire())
            this.context.getAutowireCapableBeanFactory().autowireBean(this);

        // Initialize subclass
        this.initSpringApplication(context);
    }

    /**
     * Should this instance itself be autowired via the associated application context?
     *
     * <p>
     * The implementation in {@link SpringContextApplication} returns true. Subclasses may override as necessary.
     * </p>
     */
    protected boolean isAutowire() {
        return true;
    }

    /**
     * Initialize the application. Sub-classes of {@link SpringContextApplication} must implement this method.
     *
     * @param context the associated {@link WebApplicationContext} just created and refreshed
     */
    protected abstract void initSpringApplication(ConfigurableWebApplicationContext context);

    /**
     * Post-process the given {@link WebApplicationContext} after initial creation but before the initial
     * {@link WebApplicationContext#refresh refresh()}.
     *
     * <p>
     * The implementation in {@link SpringContextApplication} does nothing. Subclasses may override as necessary.
     * </p>
     *
     * @param context the associated {@link WebApplicationContext} just refreshed
     * @see #onRefresh
     * @see ConfigurableWebApplicationContext#refresh()
     */
    protected void postProcessWebApplicationContext(ConfigurableWebApplicationContext context) {
    }

    /**
     * Perform any application-specific work after a successful application context refresh.
     *
     * <p>
     * The implementation in {@link SpringContextApplication} does nothing. Subclasses may override as necessary.
     * </p>
     *
     * @param context the associated {@link WebApplicationContext} just refreshed
     * @see #postProcessWebApplicationContext
     * @see WebApplicationContext#refresh
     */
    protected void onRefresh(ApplicationContext context) {
    }

    /**
     * Get the name for this application. This is used as the name of the XML file defining the application context.
     *
     * <p>
     * The implementation in {@link SpringContextApplication} returns this instance's class'
     * {@linkplain Class#getSimpleName simple name}.
     */
    protected String getApplicationName() {
        return this.getClass().getSimpleName();
    }
}

