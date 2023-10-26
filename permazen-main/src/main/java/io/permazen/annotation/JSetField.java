
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.Permazen;
import io.permazen.StorageIdGenerator;

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
 * To specify a primitive element type, specify the primitive type name (e.g., {@code "int"}) as the {@link JField#encoding}
 * in the {@link #element}.
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface JSetField {

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
     * Storage ID for this field. Value should be positive and unique within the contained class.
     * If zero, the configured {@link StorageIdGenerator} will be consulted to auto-generate a value.
     *
     * @return the set field storage ID
     * @see StorageIdGenerator#generateFieldStorageId StorageIdGenerator.generateFieldStorageId()
     */
    int storageId() default 0;

    /**
     * Storage ID and index setting for the field's elements. Note: the {@link JField#name name} property must be left unset.
     *
     * @return the set element field
     */
    JField element() default @JField();
}
