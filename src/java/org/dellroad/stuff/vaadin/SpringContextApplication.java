
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.Application;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
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
 * <h3>Vaadin Application as BeanFactory singleton</h3>
 *
 * <p>
 * This {@link SpringContextApplication} instance can itself be exposed in, and configured by, the associated Spring
 * application context. Simply create a bean definition that invokes {@link ContextApplication#get}:
 * <blockquote><pre>
 *  &lt;bean id="myVaadinApplication" class="org.dellroad.stuff.vaadin.ContextApplication" factory-method="get"/&gt;
 * </pre></blockquote>
 *
 * <p>
 * This then allows you to autowire the {@link SpringContextApplication} and other UI components together, e.g.:
 * <blockquote><pre>
 *  public class MyApplication extends SpringContextApplication {
 *
 *      &#64;Autowired
 *      private MainPanel mainPanel;
 *
 *      &#64;Override
 *      public void initSpringApplication(ConfigurableWebApplicationContext context) {
 *          this.mainWindow = new Window("MyApplication", this.mainPanel);
 *          this.setMainWindow(this.mainWindow);
 *      }
 *
 *      ...
 *  }
 *
 *  &#64;Component
 *  public class MainPanel extends VerticalLayout {
 *
 *      &#64;Autowired
 *      private MyApplication application;
 *
 *      ...
 *  }
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * Even if you don't explicitly define the {@link SpringContextApplication} bean in your Spring application context,
 * it will still be available as a dependency for autowiring into other beans (this is accomplished using
 * {@link ConfigurableListableBeanFactory#registerResolvableDependency
 * ConfigurableListableBeanFactory.registerResolvableDependency()}). Of course, in this case the
 * {@link SpringContextApplication} bean won't itself be autowired or configured.
 * </p>
 *
 * <h3><code>@VaadinConfigurable</code> Beans</h3>
 *
 * <p>
 * It is also possible to configure beans outside of this application context using AOP, so that any invocation of
 * {@code new FooBar()}, where the class {@code FooBar} is marked {@link VaadinConfigurable @VaadinConfigurable},
 * will automagically cause the new {@code FooBar} object to be configured by the application context associated with
 * the {@linkplain ContextApplication#get() currently running application instance}. In effect, this does for
 * Vaadin application beans what Spring's {@link org.springframework.beans.factory.annotation.Configurable @Configurable}
 * does for regular beans.
 * </p>
 *
 * <p>
 * Note however that Spring {@linkplain org.springframework.beans.factory.DisposableBean#destroy destroy methods}
 * will not be invoked on application close for these beans, since their lifecycle is controlled outside of the
 * Spring application context (this is also the case with
 * {@link org.springframework.beans.factory.annotation.Configurable @Configurable} beans). Instead, these beans
 * can register as a {@link ContextApplication.CloseListener} for shutdown notification.
 * </p>
 *
 * <p>
 * For the this annotation to do anything, {@link VaadinConfigurable @VaadinConfigurable} classes must be woven
 * (either at build time or runtime) using the
 * <a href="http://www.eclipse.org/aspectj/doc/released/faq.php#compiler">AspectJ compiler</a> with the
 * {@code VaadinConfigurableAspect} aspect (included in the <code>dellroad-stuff</code> JAR file).
 * </p>
 *
 * @see ContextApplication#get
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
     * @see #destroySpringApplication
     */
    protected abstract void initSpringApplication(ConfigurableWebApplicationContext context);

    /**
     * Perform any application-specific shutdown work. This will be invoked at shutdown after this Vaadin application and the
     * associated {@link WebApplicationContext} have both been closed.
     *
     * <p>
     * The implementation in {@link SpringContextApplication} does nothing. Subclasses may override as necessary.
     * </p>
     *
     * <p>
     * Note that if a {@link SpringContextApplication} instance is exposed in the application context and configured
     * with a Spring {@linkplain org.springframework.beans.factory.DisposableBean#destroy destroy method}, then that
     * method will also be invoked when the application is closed. In such cases overriding this method is not necessary.
     * </p>
     *
     * @see #initSpringApplication
     */
    protected void destroySpringApplication() {
    }

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

        // Register this instance as an implicitly resolvable dependency
        this.context.addBeanFactoryPostProcessor(new BeanFactoryPostProcessor() {
            @Override
            public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
                beanFactory.registerResolvableDependency(Application.class, SpringContextApplication.this);
            }
        });

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
            SpringContextApplication.this.destroySpringApplication();
        }
    }
}

