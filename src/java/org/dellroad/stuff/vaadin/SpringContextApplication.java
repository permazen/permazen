
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.wiring.BeanConfigurerSupport;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.SourceFilteringListener;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.context.support.XmlWebApplicationContext;

/**
 * Vaadin application implementation that manages an associated Spring {@link WebApplicationContext}.
 *
 * <h3>Overview</h3>
 *
 * <p>
 * Each Vaadin application instance is given its own Spring application context, and all such
 * application contexts share the same parent context, which is the one associated with the overal servlet web context
 * (i.e., the one created by Spring's {@link org.springframework.web.context.ContextLoaderListener ContextLoaderListener}).
 * A context is created when a new Vaadin application instance is initialized, and destroyed when it is closed.
 * </p>
 *
 * <p>
 * This setup is analogous to how Spring's {@link org.springframework.web.servlet.DispatcherServlet DispatcherServlet}
 * creates per-servlet application contexts that are children of the overall servlet web context.
 * </p>
 *
 * <p>
 * For each Vaadin application {@code com.example.FooApplication} that subclasses this class, there should exist an XML
 * file named {@code FooApplication.xml} in the {@code WEB-INF/} directory that defines the per-Vaadin application Spring
 * application context (this naming scheme {@linkplain #getApplicationName can be overriden}).
 * </p>
 *
 * <h3>Application as Bean</h3>
 *
 * <p>
 * This {@link SpringContextApplication} instance can itself be included in, and optionally configured by, the associated Spring
 * application context by using a {@link ContextApplicationFactoryBean}:
 *
 * <blockquote><pre>
 *      &lt;bean class="org.dellroad.stuff.vaadin.ContextApplicationFactoryBean" p:autowire="true"/&gt;
 * </pre></blockquote>
 * If the {@code autowire} property is set to {@code true}, then the {@link SpringContextApplication} instance
 * (which already exists when the application context is created) will be autowired by the application context as well.
 * </p>
 *
 * <h3>@VaadinConfigurable Beans</h3>
 *
 * <p>
 * It is also possible to configure beans outside of this application context using AOP, so that any invocation of
 * {@code new FooBar()}, where the class {@code FooBar} is marked {@link VaadinConfigurable @VaadinConfigurable},
 * will automagically cause the new {@code FooBar} object to be autowired by the application context associated with
 * the {@linkplain ContextApplication#get() currently running application instance}.
 * This includes lifecycle management; for example, any Spring
 * {@linkplain org.springframework.beans.factory.DisposableBean#destroy destroy-method} will be invoked on application close.
 * </p>
 *
 * <p>
 * For this to work, {@link VaadinConfigurable @VaadinConfigurable} classes must be woven
 * (either at build time or runtime) using the
 * <a href="http://www.eclipse.org/aspectj/doc/released/faq.php#compiler">AspectJ compiler</a> with the
 * {@code VaadinConfigurableAspect} aspect (included in the <code>dellroad-stuff</code> JAR file).
 * </p>
 *
 * @see ContextApplication#get
 * @see ContextApplicationFactoryBean
 * @see <a href="https://github.com/archiecobbs/dellroad-stuff-vaadin-spring-demo3">Example Code on GitHub</a>
 */
@SuppressWarnings("serial")
public abstract class SpringContextApplication extends ContextApplication {

    private static final AtomicLong UNIQUE_INDEX = new AtomicLong();

    private transient ConfigurableWebApplicationContext context;

    /**
     * Get this instance's associated Spring application context.
     */
    public ConfigurableWebApplicationContext getApplicationContext() {
        return this.context;
    }

    /**
     * Configure a bean using this instance's associated application context.
     * Intended for use by aspects.
     */
    protected void configureBean(Object bean) {
        BeanConfigurerSupport beanConfigurerSupport = new BeanConfigurerSupport();
        beanConfigurerSupport.setBeanFactory(this.context.getBeanFactory());
        beanConfigurerSupport.setBeanWiringInfoResolver(new VaadinConfigurableBeanWiringInfoResolver());
        beanConfigurerSupport.afterPropertiesSet();
        beanConfigurerSupport.configureBean(bean);
        beanConfigurerSupport.destroy();
    }

    /**
     * Get the {@link SpringContextApplication} instance associated with the current thread or throw an exception if there is none.
     *
     * <p>
     * Works just like {@link ContextApplication#get()} but returns this narrower type.
     * </p>
     *
     * @return the {@link SpringContextApplication} associated with the current thread
     * @throws IllegalStateException if the current thread is not servicing a Vaadin web request
     *  or the current Vaadin {@link com.vaadin.Application} is not a {@link SpringContextApplication}
     */
    public static SpringContextApplication get() {
        return ContextApplication.get(SpringContextApplication.class);
    }

    /**
     * Initializes the associated {@link ConfigurableWebApplicationContext}.
     */
    protected final void initApplication() {

        // Load the context
        this.loadContext();

        // Initialize subclass
        this.initSpringApplication(context);
    }

    /**
     * Initialize the application. Sub-classes of {@link SpringContextApplication} must implement this method.
     *
     * @param context the associated {@link WebApplicationContext} just created and refreshed
     */
    protected abstract void initSpringApplication(ConfigurableWebApplicationContext context);

    /**
     * Post-process the given {@link WebApplicationContext} after initial creation but before the initial
     * {@link org.springframework.context.ConfigurableApplicationContext#refresh refresh()}.
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
     * @see org.springframework.context.ConfigurableApplicationContext#refresh
     */
    protected void onRefresh(ApplicationContext context) {
    }

    /**
     * Get the name for this application. This is used as the name of the XML file in {@code WEB-INF/} that
     * defines the Spring application context associated with this instance.
     *
     * <p>
     * The implementation in {@link SpringContextApplication} returns this instance's class'
     * {@linkplain Class#getSimpleName simple name}.
     * </p>
     */
    protected String getApplicationName() {
        return this.getClass().getSimpleName();
    }

// ApplicationContext setup

    private void loadContext() {

        // Logging
        this.log.info("loading application context for Vaadin application " + this.getApplicationName());

        // Sanity check
        if (this.context != null)
            throw new IllegalStateException("context already loaded");

        // Find the application context associated with the servlet; it will be the parent
        ServletContext servletContext;
        HttpServletRequest request = ContextApplication.currentRequest();
        try {
            // getServletContext() is a servlet AIP 3.0 method, so don't freak out if it's not there
            servletContext = (ServletContext)HttpServletRequest.class.getMethod("getServletContext").invoke(request);
        } catch (Exception e) {
            servletContext = ContextLoader.getCurrentWebApplicationContext().getServletContext();
        }
        WebApplicationContext parent = WebApplicationContextUtils.getWebApplicationContext(servletContext);

        // Create and configure a new application context for this Application instance
        this.context = new XmlWebApplicationContext();
        this.context.setId(ConfigurableWebApplicationContext.APPLICATION_CONTEXT_ID_PREFIX
          + servletContext.getContextPath() + "/" + this.getApplicationName() + "-"
          + SpringContextApplication.UNIQUE_INDEX.incrementAndGet());
        this.context.setParent(parent);
        this.context.setServletContext(servletContext);
        //context.setServletConfig(??);
        this.context.setNamespace(this.getApplicationName());

        // Register listener so we can notify subclass on refresh events
        this.context.addApplicationListener(new SourceFilteringListener(this.context, new RefreshListener()));

        // Invoke any subclass setup
        this.postProcessWebApplicationContext(context);

        // Refresh context
        this.context.refresh();

        // Get notified of application shutdown so we can shut down the context as well
        this.addListener(new ContextCloseListener());
    }

// Serialization

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        this.loadContext();
    }

// Nested classes

    // My refresh listener
    private class RefreshListener implements ApplicationListener<ContextRefreshedEvent>, Serializable {
        @Override
        public void onApplicationEvent(ContextRefreshedEvent event) {
            SpringContextApplication.this.onRefresh(event.getApplicationContext());
        }
    }

    // My close listener
    private class ContextCloseListener implements CloseListener, Serializable {
        @Override
        public void applicationClosed(CloseEvent closeEvent) {
            SpringContextApplication.this.log.info("closing application context associated with Vaadin application "
              + SpringContextApplication.this.getApplicationName());
            SpringContextApplication.this.context.close();
        }
    }
}

