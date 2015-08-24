
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
 * Java annotation for the getter methods of Java bean properties reflecting {@link java.util.List} fields.
 *
 * <p>
 * The annotated method's return type must be {@link java.util.List List}{@code <E>}, where {@code E} is a supported simple type.
 * </p>
 *
 * <p>
 * List fields have a "random access" performance profile similar to an {@link java.util.ArrayList}. In particular,
 * {@link java.util.List#get List.get()} and {@link java.util.List#size List.size()} are constant time, but an insertion
 * in the middle of the list requires shifting all subsequent values by one.
 * </p>
 *
 * <p>
 * Note that both primitive types and their corresponding wrapper types are supported as elements. A list whose
 * elements have primitive type will throw an exception on an attempt to add a null value.
 * To specify a primitive element type, specify the primitive type name (e.g., {@code "int"})
 * as the {@link JField#type} in the {@link #element}.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface JListField {

    /**
     * The name of this field.
     *
     * <p>
     * If empty string (default value), the name is inferred from the name of the annotated Java bean getter method.
     * </p>
     *
     * @return the list field name
     */
    String name() default "";

    /**
     * Storage ID for this field. Value should be positive and unique within the contained class.
     * If zero, the configured {@link org.jsimpledb.StorageIdGenerator} will be consulted to auto-generate a value.
     *
     * @return the list field storage ID
     * @see org.jsimpledb.StorageIdGenerator#generateFieldStorageId StorageIdGenerator.generateFieldStorageId()
     */
    int storageId() default 0;

    /**
     * Storage ID and index setting for the field's elements. Note: the {@link JField#name name} property must be left unset.
     *
     * @return the list element field
     */
    JField element() default @JField();
}

