
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.annotation.Autowire;

/**
 * Indicates that the class is a candidate for configuration using the {@code VaadinConfigurableAspect} aspect.
 *
 * <p>
 * Analogous to Spring's {@link org.springframework.beans.factory.annotation.Configurable @Configurable} annotation,
 * but causes beans to be autowired into the Spring application context associated with the current
 * {@link SpringContextApplication} Vaadin application instead of the Spring application context associated
 * with the servlet context.
 * </p>
 *
 * <p>
 * Running the AspectJ compiler on your annotated classes is required to activate this annotation.
 * </p>
 *
 * @see org.dellroad.stuff.vaadin
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
}

