
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
 * Container for the {@link java.lang.annotation.Repeatable &#64;Repeatable} annotation {@link PermazenCompositeIndex}.
 *
 * @see PermazenCompositeIndex
 * @see <a href="https://docs.oracle.com/javase/tutorial/java/annotations/repeating.html">Repeating Annotations</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.TYPE })
@Documented
public @interface PermazenCompositeIndexes {
    PermazenCompositeIndex[] value();
}
