
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

import org.jsimpledb.core.DeleteAction;

/**
 * Java annotation for creating simple fields, including reference fields that refer to other Java model object types.
 *
 * <p>
 * This annotation is used in two scenarios:
 * <ul>
 *  <li>To describe a simple database field by annotating the corresponding abstract Java bean property `getter' method</li>
 *  <li>To describe the sub-field(s) of a complex database field (i.e., set, list, or map),
 *      i.e., the collection element or map key and value</li>
 * </ul>
 * </p>
 *
 * <p>
 * Note that this annotation can be applied to superclass and interface methods to have the corresponding
 * field defined in all sub-types.
 * </p>
 *
 * <p>
 * This annotation is not required when auto-generation of properties is enabled; see {@link JSimpleClass#autogenFields}.
 * </p>
 *
 * <p><b>Non-Reference Fields</b></p>
 *
 * <p>
 * If the field is not a reference field, the property type is inferred from the type of the annotated method or,
 * in the case of complex sub-fields, the generic type of the collection class. The name of the property type
 * must be registered in the {@link org.jsimpledb.core.FieldTypeRegistry} (perhaps via {@link JFieldType &#64;JFieldType}),
 * and the corresponding {@link org.jsimpledb.core.FieldType} is then used to encode/decode field values.
 * The type name may also be specified explicitly by {@link #name}.
 * </p>
 *
 * <p>
 * Simple fields may be {@link #indexed}; see {@link org.jsimpledb.index} for information on querying indexes.
 * </p>
 *
 * <p><b>Reference Fields</b></p>
 *
 * <p>
 * If the type of the field is (assignable to) a {@link JSimpleClass &#64;JsimpleClass}-annotated Java model object type,
 * then the field is a reference field.
 * </p>
 *
 * <p>
 * Reference fields have configurable behavior when the referred-to object is deleted; see {@link #onDelete}.
 * </p>
 *
 * <p>
 * Reference fields are always indexed; the value of {@link #indexed} is ignored.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface JField {

    /**
     * The name of this field.
     *
     * <p>
     * If equal to the empty string (default value), the name is inferred from the name of the annotated Java bean getter method.
     * </p>
     *
     * <p>
     * For sub-fields of complex fields, this property must be left unset.
     * </p>
     */
    String name() default "";

    /**
     * The type of this field.
     *
     * <p>
     * If set, this must equal the name of a type registered in the {@link org.jsimpledb.core.FieldTypeRegistry}
     * associated with the {@link org.jsimpledb.core.Database} instance, and the annotated method's return type must match the
     * {@link org.jsimpledb.core.FieldType}'s {@linkplain org.jsimpledb.core.FieldType#getTypeToken supported Java type}.
     *
     * <p>
     * If equal to the empty string (default value), then the Java type is inferred from the return type of the getter method
     * and the {@link org.jsimpledb.core.FieldType} is found via
     * {@link org.jsimpledb.core.FieldTypeRegistry#getFieldType(com.google.common.reflect.TypeToken)
     * FieldTypeRegistry.getFieldType()}.
     * </p>
     *
     * <p>
     * For reference fields (i.e., methods with return value equal to a {@link JSimpleClass &#64;JSimpleClass}-annotated class),
     * this property must be left unset.
     * </p>
     *
     * <p>
     * For sub-fields of complex fields, this property can be used to force a primitive sub-field type instead of a
     * primitive wrapper type. In that case, the complex field will disallow nulls.
     * </p>
     *
     * <p>
     * For example:
     * <pre>
     * &#64;JSimpleClass(storageId = 10)
     * public class Team {
     *
     *     &#64;JSetField(storageId = 11,
     *       element = &#64;JField(storageId = 12, <b>type = "float"</b>)) // nulls will be disallowed
     *     public abstract List&lt;<b>Float</b>&gt; getScores();
     * }
     * </pre>
     * </p>
     */
    String type() default "";

    /**
     * Storage ID for this field. Value should be positive and unique within the contained class.
     * If zero, the configured {@link org.jsimpledb.StorageIdGenerator} will be consulted to auto-generate a value
     * unless {@link JSimpleClass#autogenFields} is false (in which case an error occurs).
     *
     * @see org.jsimpledb.StorageIdGenerator#generateFieldStorageId StorageIdGenerator.generateFieldStorageId()
     * @see org.jsimpledb.StorageIdGenerator#generateSetElementStorageId StorageIdGenerator.generateSetElementStorageId()
     * @see org.jsimpledb.StorageIdGenerator#generateListElementStorageId StorageIdGenerator.generateListElementStorageId()
     * @see org.jsimpledb.StorageIdGenerator#generateMapKeyStorageId StorageIdGenerator.generateMapKeyStorageId()
     * @see org.jsimpledb.StorageIdGenerator#generateMapValueStorageId StorageIdGenerator.generateMapValueStorageId()
     */
    int storageId() default 0;

    /**
     * Whether this field is indexed or not.
     *
     * <p>
     * Setting this property to true creates a simple index on this field. To have this field participate in
     * a composite index on multiple fields, use {@link JSimpleClass#compositeIndexes}.
     * </p>
     *
     * <p>
     * Note: reference fields are always indexed (for reference fields, this property is ignored).
     * </p>
     */
    boolean indexed() default false;

    /**
     * For reference fields, configure the behavior when a referred-to object is
     * {@linkplain org.jsimpledb.JObject#delete deleted}.
     *
     * <p>
     * This field is ignored for non-reference fields.
     * </p>
     */
    DeleteAction onDelete() default DeleteAction.EXCEPTION;
}

