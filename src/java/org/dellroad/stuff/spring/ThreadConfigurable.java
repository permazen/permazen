
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.annotation.Autowire;

/**
 * Indicates that the class is a candidate for configuration using the {@code ThreadConfigurableAspect} aspect.
 *
 * <p>
 * Works just like Spring's {@link org.springframework.beans.factory.annotation.Configurable @Configurable} annotation,
 * but whereas {@link org.springframework.beans.factory.annotation.Configurable @Configurable} autowires using a fixed
 * bean factory stored in a static variable, {@link ThreadConfigurable @ThreadConfigurable} allows the bean factory
 * that is used for autowiring beans to be specified on a per-thread basis,
 * via {@link ThreadConfigurableBeanFactory#set ThreadConfigurableBeanFactory.set()}.
 * </p>
 *
 * <p>
 * Note: some bean factory must be specified: if a {@link ThreadConfigurable @ThreadConfigurable}-annotated bean is
 * constructed and no application context has been configured for the current thread, then an {@link IllegalStateException}
 * will be thrown. By default, the configured bean factory is inherited by spawned child threads, so typically this
 * configuration need only be done once when starting new some process or operation.
 * </p>
 *
 * @see ThreadConfigurableBeanFactory
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Inherited
public @interface ThreadConfigurable {

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

