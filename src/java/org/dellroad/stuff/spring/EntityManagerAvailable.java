
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

import org.springframework.transaction.annotation.Propagation;

/**
 * Indicates how an {@link javax.persistence.EntityManager} should be opened for the duration of the annotated method.
 * This works like Spring's {@link org.springframework.transaction.annotation.Transactional @Transactional} annotation
 * except it applies to the thread-local {@link javax.persistence.EntityManager}.
 *
 * <p>
 * This annotation is handy in setups where the container does not provide an {@link javax.persistence.EntityManager}.
 *
 * <p>
 * To activate this annotation, an {@link EntityManagerAvailableAspect} bean must exist in the application context
 * and <code>@AspectJ</code> support must be enabled. For example:
 * <blockquote><pre>
 *  &lt;!-- enable AOP support --&gt;
 *  &lt;aop:aspectj-autoproxy/&gt;
 *
 *  &lt;!-- activate @EntityManagerAvailable annotations --&gt;
 *  &lt;bean class="org.dellroad.stuff.spring.EntityManagerAvailableAspect"
 *    entityManagerFactory-ref="myEntityManagerFactory"/&gt;
 * </pre></blockquote>
 * </p>
 *
 * @see EntityManagerAvailableAspect
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface EntityManagerAvailable {

    /**
     * Propagation specification. This determines when to create a new {@link javax.persistence.EntityManager}
     * and how to handle the situation when one already exists (or not).
     */
    Propagation value() default Propagation.REQUIRED;
}

