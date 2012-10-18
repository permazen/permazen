
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.dellroad.stuff.java.ErrorAction;
import org.springframework.beans.factory.annotation.Autowire;

/**
 * Indicates that the class is a candidate for configuration using the {@code VaadinConfigurableAspect} aspect.
 *
 * <p>
 * Analogous to Spring's {@link org.springframework.beans.factory.annotation.Configurable @Configurable} annotation,
 * but causes beans to be autowired into the Spring application context associated with the current
 * {@link com.vaadin.server.VaadinServiceSession} (aka "Vaadin application") by {@link SpringServiceSession} instead of
 * the Spring application context associated with the servlet context.
 * </p>
 *
 * <p>
 * For an extra safety check, consider setting {@link #ifSessionNotLocked} where appropriate.
 * </p>
 *
 * <p>
 * For the this annotation to function properly, {@link VaadinConfigurable @VaadinConfigurable} classes must be woven
 * (either at build time or runtime) using the
 * <a href="http://www.eclipse.org/aspectj/doc/released/faq.php#compiler">AspectJ compiler</a> with the
 * {@code VaadinConfigurableAspect} aspect (included in the <code>dellroad-stuff</code> JAR file), and the
 * {@link SpringVaadinServlet} must be used.
 * </p>
 *
 * @see org.dellroad.stuff.vaadin
 * @see SpringVaadinServlet
 * @see SpringServiceSession
 * @see <a href="https://github.com/archiecobbs/dellroad-stuff-vaadin-spring-demo3/tree/vaadin7">Example Code on GitHub</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Inherited
public @interface VaadinConfigurable {

    /**
     * Configuration bean definition template name, if any.
     */
    String value() default "";

    /**
     * Whether and how to automatically autowire dependencies.
     */
    Autowire autowire() default Autowire.NO;

    /**
     * Whether to enable dependency checking.
     */
    boolean dependencyCheck() default false;

    /**
     * Whether to inject dependencies prior to constructor execution.
     */
    boolean preConstruction() default false;

    /**
     * What to do when we discover that the {@link com.vaadin.server.VaadinServiceSession} is not
     * {@linkplain com.vaadin.server.VaadinServiceSession#getLock locked} when the annotated bean is constructed.
     * For beans that are (or will interact with) Vaadin widgets, containers, etc., this typically
     * indicates a programming error. In such cases, this property configures what to do, if anything.
     *
     * @see com.vaadin.server.VaadinServiceSession#getLock
     */
    ErrorAction ifSessionNotLocked() default ErrorAction.IGNORE;
}

