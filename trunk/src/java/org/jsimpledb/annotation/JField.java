
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
 *  <li>To describe a <b>simple</b> database field by annotating the corresponding abstract Java bean property `getter' method</li>
 *  <li>To describe the <b>sub-field(s)</b> of a <b>complex</b> database field (i.e., set, list, or map),
 *      that is, the collection element or map key and value</li>
 * </ul>
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
 * Reference fields have configurable behavior when the containing object or the referred-to object is deleted;
 * see {@link #onDelete} and {@link #cascadeDelete}.
 * </p>
 *
 * <p>
 * Reference fields are always indexed; the value of {@link #indexed} is ignored.
 * </p>
 *
 * <p><b>Uniqueness Constraints</b></p>
 *
 * <p>
 * Fields that are not complex sub-fields may be marked as {@link #unique} to impose a uniqueness constraint on the value.
 * Fields with uniqueness constraints must be indexed. Uniqueness constraints are handled at the JSimpleDB layer and function as
 * an implicit validation constraint. In other words, the constraint is verified when the validation queue is processed
 * and is affected by the transaction's configured {@link org.jsimpledb.ValidationMode}.
 * </p>
 *
 * <p>
 * Optionally, specific field values may be marked as excluded from the uniqueness constraint via {@link #uniqueExclude}.
 * If so, the specified values may appear in multiple objects without violating the constraint. Because null values
 * are not allowed in annotations, {@link #uniqueExcludeNull} is provided to exclude the null value.
 * </p>
 *
 * <p>
 * In {@link org.jsimpledb.ValidationMode#AUTOMATIC}, any upgraded {@link org.jsimpledb.JObject}s are automatically
 * added to the validation queue, so a uniqueness constraint added in a new schema version will be automatically verified
 * when any object is upgraded.
 * However, simpley adding, removing, or changing uniqueness constraints on an existing field doesn't force a schema
 * version change. Therefore, existing database objects that were previously valid could be in violation of a uniqueness
 * constraint that is added without one. To avoid this possibililty, change the schema version number as well.
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
     *
     * @return the name of the field
     */
    String name() default "";

    /**
     * Optional override for the type of this field.
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
     * primitive wrapper type. In that case, the complex field will disallow null values. For example:
     * <pre>
     *  &#64;JSetField(element = &#64;JField(<b>type = "float"</b>)) // nulls will be disallowed
     *  public abstract List&lt;<b>Float</b>&gt; getScores();
     * </pre>
     *
     * @return the name of the field's type
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
     *
     * @return the field's storage ID
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
     *
     * @return whether the field is indexed
     */
    boolean indexed() default false;

    /**
     * For reference fields, configure the behavior when the referred-to object is
     * {@linkplain org.jsimpledb.JObject#delete deleted}.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     * </p>
     *
     * @return desired behavior when a referenced object is deleted
     * @see #cascadeDelete
     * @see org.jsimpledb.JObject#delete
     */
    DeleteAction onDelete() default DeleteAction.EXCEPTION;

    /**
     * For reference fields, configure cascading behavior when the containing object is
     * {@linkplain org.jsimpledb.JObject#delete deleted}. If set to true, the referred-to object
     * is automatically deleted as well.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     * </p>
     *
     * @return whether deletion should cascade to the referred-to object
     * @see #onDelete
     * @see org.jsimpledb.JObject#delete
     */
    boolean cascadeDelete() default false;

    /**
     * Require this field's value to be unique among all database objects.
     *
     * <p>
     * This property must be false for sub-fields of complex fields.
     * </p>
     *
     * @return whether the field's value should be unique
     * @see #uniqueExclude
     * @see #uniqueExcludeNull
     */
    boolean unique() default false;

    /**
     * Specify field value(s) which are excluded from the uniqueness constraint.
     *
     * <p>
     * The specified values must be valid {@link String} encodings of the associated field. For example:
     * <pre>
     *  &#64;JField(indexed = true, unique = true, uniqueExclude = { "Infinity", "-Infinity" })
     *  public abstract float getPriority();
     * </pre>
     *
     * <p>
     * This property must be left empty when {@link #unique} is false.
     * </p>
     *
     * @return values to exclude from the uniqueness constraint
     * @see #unique
     * @see #uniqueExcludeNull
     */
    String[] uniqueExclude() default {};

    /**
     * Specify that the null value is excluded from the uniqueness constraint.
     *
     * <p>
     * This property must be left false when {@link #unique} is false or the field has primitive type.
     * </p>
     *
     * @return whether null should be excluded from the uniqueness constraint
     * @see #unique
     * @see #uniqueExclude
     */
    boolean uniqueExcludeNull() default false;
}

