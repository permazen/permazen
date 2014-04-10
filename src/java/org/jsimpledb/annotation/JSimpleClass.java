
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for Java classes that are {@link org.jsimpledb.jlayer.JLayer} object model types.
 *
 * <p>
 * Typically such classes are declared {@code abstract} and contain {@code abstract} Java bean getter methods
 * with {@link JField &#64;JField}, {@link JSetField &#64;JSetField}, etc. annotations that
 * define the fields of the object type. The generated subclass will implement {@link org.jsimpledb.jlayer.JObject},
 * so the annotated class may be declared that way. The annotated class must have a zero-parameter constructor.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface JSimpleClass {

    /**
     * The name of this object type.
     *
     * <p>
     * If equal to the empty string (default value),
     * the {@linkplain Class#getSimpleName simple name} of the annotated Java class is used.
     * </p>
     */
    String name() default "";

    /**
     * Storage ID for this object type. Value must be positive.
     */
    int storageId();
}

