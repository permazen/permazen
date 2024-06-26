
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.Permazen;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for the getter methods of Java bean properties reflecting {@link Permazen}
 * {@link java.util.Set} fields.
 *
 * <p>
 * The annotated method's return type must be either {@link java.util.Set Set}{@code <E>},
 * {@link java.util.SortedSet SortedSet}{@code <E>}, or {@link java.util.NavigableSet NavigableSet}{@code <E>},
 * where {@code E} is a supported simple type.
 *
 * <p>
 * Note that both primitive types and their corresponding wrapper types are supported as elements. A set whose
 * elements have primitive type will throw an exception on an attempt to add a null value.
 * To specify a primitive element type, specify the primitive type name (e.g., {@code "int"}) as the {@link PermazenField#encoding}
 * in the {@link #element}.
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring-framework/reference/core/beans/classpath-scanning.html#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface PermazenSetField {

    /**
     * The name of this field.
     *
     * <p>
     * If empty string (default value), the name is inferred from the name of the annotated Java bean getter method.
     *
     * @return the set field name
     */
    String name() default "";

    /**
     * Storage ID for this field.
     *
     * <p>
     * Normally this value is left as zero, in which case a value will be automatically assigned.
     *
     * <p>
     * Otherwise, the value should be positive and unique within the contained class.
     *
     * @return the field's storage ID, or zero for automatic assignment
     */
    int storageId() default 0;

    /**
     * Configuration for the field's elements.
     *
     * <p>
     * Normally this property only needs to be set to index the sub-field.
     * If set, the {@link PermazenField#name name} property must be left unset.
     *
     * @return configuration for the set element sub-field
     */
    PermazenField element() default @PermazenField();
}
