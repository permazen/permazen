
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Eliminates the annotated method from consideration for Permazen field auto-generation.
 *
 * <p>
 * This annotation is ignored on methods that also have a {@link PermazenField &#64;PermazenField}, {@link PermazenSetField &#64;PermazenSetField},
 * {@link PermazenListField &#64;PermazenListField}, or {@link PermazenMapField &#64;PermazenMapField} annotation.
 *
 * <p>
 * It is only useful on non-abstract methods in classes for which both {@link PermazenType#autogenFields}
 * and {@link PermazenType#autogenNonAbstract} are true.
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 *
 * @see PermazenType#autogenFields
 * @see PermazenType#autogenNonAbstract
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface PermazenTransient {
}
