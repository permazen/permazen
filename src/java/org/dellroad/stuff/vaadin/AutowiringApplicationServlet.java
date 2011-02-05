
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.Application;
import com.vaadin.terminal.gwt.server.ApplicationServlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * {@link ApplicationServlet} that autowires and configures the {@link Application}
 * objects it creates using the associated Spring {@link WebApplicationContext}.
 * This allows a Vaadin application to be configured normally as a Spring bean.
 *
 * <p>
 * For example, annotations such as
 * <code>{@link org.springframework.beans.factory.annotation.Autowired @Autowired}</code>,
 * <code>{@link org.springframework.beans.factory.annotation.Required @Required}</code>, etc.
 * and interfaces such as {@link org.springframework.beans.factory.BeanFactoryAware BeanFactoryAware},
 * etc. will work on your {@link Application} instances.
 * </p>
 *
 * <p>
 * An example of "direct" use of this servlet in conjunction with Spring's
 * {@link org.springframework.web.context.ContextLoaderListener ContextLoaderListener}:
 * <blockquote><pre>
 *  &lt;!-- Spring context loader --&gt;
 *  &lt;listener&gt;
 *      &lt;listener-class&gt;org.springframework.web.context.ContextLoaderListener&lt;/listener-class&gt;
 *  &lt;/listener&gt;
 *
 *  &lt;!-- Vaadin servlet --&gt;
 *  &lt;servlet&gt;
 *      &lt;servlet-name&gt;myapp&lt;/servlet-name&gt;
 *      &lt;servlet-class&gt;com.example.AutowiringApplicationServlet&lt;/servlet-class&gt;
 *      &lt;init-param&gt;
 *          &lt;param-name&gt;application&lt;/param-name&gt;
 *          &lt;param-value&gt;some.spring.configured.Application&lt;/param-value&gt;
 *      &lt;/init-param&gt;
 *      &lt;init-param&gt;
 *          &lt;param-name&gt;productionMode&lt;/param-name&gt;
 *          &lt;param-value&gt;true&lt;/param-value&gt;
 *      &lt;/init-param&gt;
 *  &lt;/servlet&gt;
 *  &lt;servlet-mapping&gt;
 *      &lt;servlet-name&gt;myapp&lt;/servlet-name&gt;
 *      &lt;url-pattern&gt;/*&lt;/url-pattern&gt;
 *  &lt;/servlet-mapping&gt;
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * An example that creates a Spring MVC "controller" bean for use with Spring's
 * {@link org.springframework.web.servlet.DispatcherServlet DispatcherServlet}:
 * <blockquote><pre>
 *  &lt;!-- Activate Spring annotation support --&gt;
 *  &lt;context:annotation-config/&gt;
 *
 *  &lt;!-- Define controller bean for Vaadin application --&gt;
 *  &lt;bean id="applicationController" class="org.springframework.web.servlet.mvc.ServletWrappingController"
 *     p:servletClass="com.example.AutowiringApplicationServlet"&gt;
 *      &lt;property name="initParameters"&gt;
 *          &lt;props&gt;
 *              &lt;prop key="application"&gt;some.spring.configured.Application&lt;/prop&gt;
 *              &lt;prop key="productionMode"&gt;true&lt;/prop&gt;
 *          &lt;/props&gt;
 *      &lt;/property&gt;
 *  &lt;/bean&gt;
 * </pre></blockquote>
 *
 * @see org.springframework.web.servlet.mvc.ContextLoaderListener
 * @see org.springframework.web.servlet.DispatcherServlet
 * @see org.springframework.web.servlet.mvc.ServletWrappingController
 * @see AutowireCapableBeanFactory
 */
@SuppressWarnings("serial")
public class AutowiringApplicationServlet extends ApplicationServlet {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private WebApplicationContext webApplicationContext;

    /**
     * Initialize this servlet.
     *
     * @throws ServletException if there is no {@link WebApplicationContext} associated with this servlet's context
     */
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        log.debug("finding containing WebApplicationContext");
        try {
            this.webApplicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext());
        } catch (IllegalStateException e) {
            throw new ServletException("could not locate containing WebApplicationContext");
        }
    }

    /**
     * Get the containing Spring {@link WebApplicationContext}.
     * This only works after the servlet has been initialized (via {@link #init init()}).
     *
     * @throws ServletException if the operation fails
     */
    protected final WebApplicationContext getWebApplicationContext() throws ServletException {
        if (this.webApplicationContext == null)
            throw new ServletException("can't retrieve WebApplicationContext before init() is invoked");
        return this.webApplicationContext;
    }

    /**
     * Get the {@link AutowireCapableBeanFactory} associated with the containing Spring {@link WebApplicationContext}.
     * This only works after the servlet has been initialized (via {@link #init init()}).
     *
     * @throws ServletException if the operation fails
     */
    protected final AutowireCapableBeanFactory getAutowireCapableBeanFactory() throws ServletException {
        try {
            return getWebApplicationContext().getAutowireCapableBeanFactory();
        } catch (IllegalStateException e) {
            throw new ServletException("containing context " + getWebApplicationContext() + " is not autowire-capable", e);
        }
    }

    /**
     * Create and configure a new instance of the configured application class.
     *
     * <p>
     * The implementation in {@link AutowiringApplicationServlet} delegates to
     * {@link #getAutowireCapableBeanFactory getAutowireCapableBeanFactory()}, then invokes
     * {@link AutowireCapableBeanFactory#createBean AutowireCapableBeanFactory.createBean()}
     * using the configured {@link Application} class.
     * </p>
     *
     * @param request the triggering {@link HttpServletRequest}
     * @throws ServletException if creation or autowiring fails
     */
    @Override
    protected Application getNewApplication(HttpServletRequest request) throws ServletException {
        Class<? extends Application> cl;
        try {
            cl = getApplicationClass();
        } catch (ClassNotFoundException e) {
            throw new ServletException("failed to create new instance of application class", e);
        }
        log.debug("creating new instance of " + cl);
        AutowireCapableBeanFactory beanFactory = getAutowireCapableBeanFactory();
        try {
            return beanFactory.createBean(cl);
        } catch (BeansException e) {
            throw new ServletException("failed to create new instance of " + cl, e);
        }
    }
}

