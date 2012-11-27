
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
 * <li>Autowiring of Spring-configured beans (both per-Vaadin application and "servlet global") into Vaadin beans</li>
 * <li>An AOP aspect that allows {@link org.dellroad.stuff.vaadin7.VaadinConfigurable @VaadinConfigurable}
 *      application beans to be autowired on instantiation using the curent Vaadin application's Spring application context</li>
 * <li>A safe and clearly defined interface for background threads needing to interact with a Vaadin application</li>
 * <li>A custom Spring scope that matches Vaadin application scope</li>
 * <li>Support for Vaadin application beans as listeners on non-Vaadin application event sources</li>
 * </ul>
 * </p>
 *
 * <p>
 * Key classes include:
 * <ul>
 *  <li>{@link org.dellroad.stuff.vaadin7.SpringVaadinServlet} associates a Spring application context
 *      with each "Vaadin application" instance (i.e., each {@link com.vaadin.server.VaadinSession}) using
 *      the {@link org.dellroad.stuff.vaadin7.SpringVaadinSession} class (which does the bulk of the work).
 *      The parent context is the application context associated with the overall servlet context already set up by Spring.
 *      This is analogous to what Spring's {@link org.springframework.web.servlet.DispatcherServlet} does
 *      for each servlet instance. Beans inside this context have a lifecycle which exactly tracks the Vaadin application,
 *      eliminating a major source of thread and memory leaks.</li>
 *  <li>An AOP aspect that works on {@link org.dellroad.stuff.vaadin7.VaadinConfigurable @VaadinConfigurable} beans,
 *      allowing them to be autowired by the Spring application context associated with the current Vaadin application
 *      instance by {@link org.dellroad.stuff.vaadin7.SpringVaadinServlet} (note: <i>not</i> just the parent context).
 *      Arbitrary beans get autowired into the Vaadin application's context at the time of construction.</li>
 *  <li>{@link org.dellroad.stuff.vaadin7.VaadinApplicationScope}, which adds a custom Spring scope so you can define application
 *      scoped beans in XML with {@code scope="vaadinApplication"} (for example, in the parent context).</li>
 *  <li>{@link org.dellroad.stuff.vaadin7.VaadinExternalListener}, a support superclass for listeners scoped to a
 *      Vaadin application that avoids memory leaks when listening to more widely scoped event sources. In particular,
 *      the subclass {@link org.dellroad.stuff.vaadin7.VaadinApplicationListener} supports listeners for Spring
 *      {@linkplain org.springframework.context.ApplicationEvent application events} multicasted in a
 *      (non-Vaadin application scoped) parent context.</li>
 *  <li>The {@link org.dellroad.stuff.vaadin7.VaadinApplication} class can serve as an application-wide singleton
 *      for a Vaadin application just like the Vaadin 6.x class of the same name used to do.
 *  <li>{@link org.dellroad.stuff.vaadin7.VaadinUtil} provides some utility and convenience methods.</li>
 * </ul>
 * </p>
 *
 * <p>
 * The classes in this package are also <a href="https://vaadin.com/directory#addon/spring-stuff">available as a Vaadin Add-on</a>
 * and <a href="https://github.com/archiecobbs/dellroad-stuff-vaadin-spring-demo3/tree/vaadin7">sample code is available on GitHub</a>.
 * </p>
 *
 * @see org.dellroad.stuff.vaadin7.SpringVaadinServlet
 * @see org.dellroad.stuff.vaadin7.VaadinConfigurable
 * @see org.dellroad.stuff.vaadin7.SpringVaadinSession
 * @see org.dellroad.stuff.vaadin7.VaadinApplicationScope
 * @see org.dellroad.stuff.vaadin7.VaadinExternalListener
 * @see org.dellroad.stuff.vaadin7.VaadinApplicationListener
 * @see <a href="http://vaadin.com/">Vaadin</a>
 * @see <a href="https://vaadin.com/directory#addon/spring-stuff">Spring Stuff Vaadin Add-on</a>
 */
package org.dellroad.stuff.vaadin7;
