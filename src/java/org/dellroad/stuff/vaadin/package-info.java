
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

/**
 * Vaadin-related classes, especially relating to Spring integration.
 *
 * <p>
 * This package contains classes that add some missing "glue" between Vaadin and Spring. In addition, these classes
 * help address the "scope mismatch" between Vaadin application scope and Spring web application context scope
 * that leads to memory leaks when a Vaadin application closes.
 * </p>
 *
 * <p>
 * The key features included are:
 * <ul>
 * <li>Automatic management of per-Vaadin application Spring application contexts, with
 *     lifecycle management of Vaadin application beans</li>
 * <li>Autowiring of Spring-configured beans (both per-application and global, including the Vaadin application itself)
 *     into Vaadin application beans</li>
 * <li>An AOP aspect that allows {@link org.springframework.beans.factory.annotation.Configurable @Configurable}
 *      application beans to be autowired on instantiation using the curent Vaadin application context</li>
 * <li>A safe and clearly defined interface for background threads needing to interact with a Vaadin application</li>
 * <li>A clearly defined interface for any thread to determine "the currently running Vaadin application"</li>
 * <li>A custom Spring scope that truly matches Vaadin application scope</li>
 * <li>Support for Vaadin application beans as listeners on non-Vaadin application event multicasters</li>
 * </ul>
 * </p>
 *
 * <p>
 * Key classes include:
 * <ul>
 *  <li>{@link org.dellroad.stuff.vaadin.ContextApplication} is a Vaadin application sub-class that provides some needed
 *      infrastructure, namely:
 *    <ul>
 *    <li>Access to the currently running Vaadin application (via a {@link org.dellroad.stuff.vaadin.ContextApplication#get()})</li>
 *    <li>A way to safely interact with a Vaadin application from a background thread (via
 *        {@link org.dellroad.stuff.vaadin.ContextApplication#invoke})</li>
 *    <li>Support for Vaadin {@linkplain org.dellroad.stuff.vaadin.ContextApplication#addListener application
 *        close event notifications}</li>
 *    </ul>
 *  </li>
 *  <li>{@link org.dellroad.stuff.vaadin.SpringContextApplication} (a sub-class of
 *      {@link org.dellroad.stuff.vaadin.ContextApplication}) that creates a new Spring application context
 *      for each new Vaadin application instance. The parent context is the application context associated with the overall
 *      servlet context. This is analogous to what Spring's {@link org.springframework.web.servlet.DispatcherServlet} does.</li>
 *  <li>A new AOP aspect that works on {@link org.springframework.beans.factory.annotation.Configurable @Configurable} beans,
 *      allowing them to be autowired by the {@link org.dellroad.stuff.vaadin.SpringContextApplication} application context
 *      associated with the current Vaadin application instance (note: <i>not</i> the parent context).</li>
 *  <li>{@link org.dellroad.stuff.vaadin.ContextApplicationFactoryBean}, a Spring factory bean that allows the
 *      {@link org.dellroad.stuff.vaadin.SpringContextApplication} Vaadin application to itself appear within
 *      (and be autowired by) its associated application context</li>
 *  <li>{@link org.dellroad.stuff.vaadin.VaadinApplicationScope}, which adds a custom Spring scope so you can define application
 *      scoped beans with {@code scope="vaadinApplication"}</li>
 *  <li>{@link org.dellroad.stuff.vaadin.VaadinApplicationListener}, a support superclass for application scoped listeners
 *      that register to receive Spring {@linkplain org.springframework.context.ApplicationEvent application events}
 *      multicasted in a (non-application scoped) parent context.</li>
 * </ul>
 * </p>
 *
 * <p>
 * The classes in this package are also <a href="https://vaadin.com/directory#addon/spring-stuff">available as a Vaadin Add-on</a>.
 * </p>
 *
 * @see <a href="http://vaadin.com/">Vaadin</a>
 * @see <a href="https://vaadin.com/directory#addon/spring-stuff">Spring Stuff Vaadin Add-on</a>
 */
package org.dellroad.stuff.vaadin;
