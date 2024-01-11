
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
 * Java annotation for the getter methods of Java bean properties reflecting {@link Permazen} {@link java.util.Map} fields.
 *
 * <p>
 * The annotated method's return type must be either {@link java.util.Map Map}{@code <K, V>},
 * {@link java.util.SortedMap SortedMap}{@code <K, V>}, or {@link java.util.NavigableMap NavigableMap}{@code <K, V>},
 * where {@code K} and {@code V} are supported simple types.
 *
 * <p>
 * Note that both primitive types and their corresponding wrapper types are supported as keys and/or values. A map whose
 * keys/values have primitive type will throw an exception on an attempt to add a null key/value.
 * To specify a primitive key or value type, specify the type name (e.g., {@code "int"}) as the {@link PermazenField#encoding}
 * in the {@link #key} or the {@link #value}.
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
public @interface PermazenMapField {

    /**
     * The name of this field.
     *
     * <p>
     * If empty string (default value), the name is inferred from the name of the annotated Java bean getter method.
     *
     * @return the map field name
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
     * Configuration for the field's keys.
     *
     * <p>
     * Normally this property only needs to be set to index the sub-field.
     * If set, the {@link PermazenField#name name} property must be left unset.
     *
     * @return configuration for the map key sub-field
     */
    PermazenField key() default @PermazenField();

    /**
     * Configuration for the field's values.
     *
     * <p>
     * Normally this property only needs to be set to index the sub-field.
     * If set, the {@link PermazenField#name name} property must be left unset.
     *
     * @return configuration for the map value sub-field
     */
    PermazenField value() default @PermazenField();
}
