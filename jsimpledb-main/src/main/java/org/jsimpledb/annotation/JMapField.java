
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for the getter methods of Java bean properties reflecting {@link org.jsimpledb.JSimpleDB}
 * {@link java.util.Map} fields.
 *
 * <p>
 * The annotated method's return type must be either {@link java.util.Map Map}{@code <K, V>},
 * {@link java.util.SortedMap SortedMap}{@code <K, V>}, or {@link java.util.NavigableMap NavigableMap}{@code <K, V>},
 * where {@code K} and {@code V} are supported simple types.
 *
 * <p>
 * Note that both primitive types and their corresponding wrapper types are supported as keys and/or values. A map whose
 * keys/values have primitive type will throw an exception on an attempt to add a null key/value.
 * To specify a primitive key or value type, specify the type name (e.g., {@code "int"}) as the {@link JField#type}
 * in the {@link #key} or the {@link #value}.
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
public @interface JMapField {

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
     * Storage ID for this field. Value should be positive and unique within the contained class.
     * If zero, the configured {@link org.jsimpledb.StorageIdGenerator} will be consulted to auto-generate a value.
     *
     * @return the map field storage ID
     * @see org.jsimpledb.StorageIdGenerator#generateFieldStorageId StorageIdGenerator.generateFieldStorageId()
     */
    int storageId() default 0;

    /**
     * Storage ID and index setting for the field's keys. Note: the {@link JField#name name} property must be left unset.
     *
     * @return the map key field
     */
    JField key() default @JField();

    /**
     * Storage ID and index setting for the field's values. Note: the {@link JField#name name} property must be left unset.
     *
     * @return the map value field
     */
    JField value() default @JField();
}

