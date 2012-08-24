
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
 * application context stored in a static variable, {@link ThreadConfigurable @ThreadConfigurable} autowires using an
 * application context that is configurable on a per-thread basis (or more generally, any way you want). This allows the
 * same {@link ThreadConfigurable @ThreadConfigurable}-annotated beans to be instantiated and autowired by different
 * application contexts at the same time, where the application context chosen depends on the thread in which
 * the instantiation is occurring. This is useful when creating multiple "parallel universe" application contexts
 * within the same application.
 * </p>
 *
 * <p>
 * The configuring application context is determined by the singleton {@link ThreadLocalContext} instance,
 * i.e., it's the value returned by invoking {@link ThreadLocalContext#getInstance} to get the singleton
 * {@link ThreadLocalContext} instance, and then {@link ThreadLocalContext#get
 * ThreadLocalContext.get()} to get the application context associated with the current thread.
 * </p>
 *
 * <p>
 * With {@link ThreadLocalContext} the configured application context is inherited by spawned child threads,
 * so typically this configuration need only be done once when starting new some process or operation,
 * even if that operation creates multiple threads.
 * </p>
 *
 * <p>
 * For example:
 *  <blockquote><pre>
 *  new Thread("context startup") {
 *      &#64;Override
 *      public void run() {
 *
 *          // Setup the context used by &#64;ThreadConfigurable beans created in this thread;
 *          // to be safe, configure the ThreadLocalContext before refreshing the context.
 *          ConfigurableApplicationContext context = new ClassPathXmlApplicationContext(
 *            new String[] { "applicationContext.xml" }, false);
 *          ThreadLocalContext.getInstance().set(context);
 *          context.refresh();
 *
 *          // Now &#64;ThreadConfigurable beans will use "context" for autowiring, but only
 *          // if they are created in this thread (or one of its child threads).
 *          new SomeThreadConfigurableBean() ...
 *      }
 *  }.start();
 *  </pre></blockquote>
 *
 * <p>
 * Note: to make this annotation behave like Spring's
 * {@link org.springframework.beans.factory.annotation.Configurable @Configurable} annotation, include the
 * {@link ThreadLocalContext} singleton instance in your bean factory:
 * <blockquote><pre>
 *     &lt;bean class="org.dellroad.stuff.spring.ThreadLocalContext" factory-method="getInstance"/&gt;
 * </pre></blockquote>
 * This will set the containing application context as the default for the singleton. This definition should be
 * listed prior to any other bean definitions that could result in {@link ThreadConfigurable @ThreadConfigurable}-annotated
 * beans being created during bean factory startup.
 * </p>
 *
 * <p>
 * Note: if a {@link ThreadConfigurable @ThreadConfigurable}-annotated bean is constructed and no application context
 * has been configured for the current thread, and there is no default set, then no configuration is performed
 * and a debug message is logged (to logger {@code org.dellroad.stuff.spring.ThreadConfigurableAspect}); this consistent
 * with the behavior of Spring's {@link org.springframework.beans.factory.annotation.Configurable @Configurable}.
 * </p>
 *
 * <p>
 * Running the AspectJ compiler on your annotated classes is required to activate this annotation.
 * </p>
 *
 * @see ThreadLocalContext
 * @see ThreadTransactional
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

