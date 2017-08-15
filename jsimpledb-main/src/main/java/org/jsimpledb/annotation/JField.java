
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.jsimpledb.UpgradeConversionPolicy;
import org.jsimpledb.core.DeleteAction;

/**
 * Java annotation for defining simple fields, including reference fields that refer to other Java model object types,
 * and {@link org.jsimpledb.Counter} fields.
 *
 * <p>
 * This annotation is used in two scenarios:
 * <ul>
 *  <li>To describe a <b>simple</b> or <b>counter</b> database field by annotating the corresponding abstract Java bean
 *      property `getter' method</li>
 *  <li>To describe the <b>sub-field</b> of a <b>complex</b> database field (i.e., set, list, or map), that is,
 *      a collection element field, or a map key or value field. In this case this annotation nests within the corresponding
 *      {@link JListField &#64;JListField}, {@link JSetField &#64;JSetField}, or {@link JMapField &#64;JMapField} annotation.</li>
 * </ul>
 *
 * <p>
 * This annotation can be applied to superclass and interface methods to have the corresponding field defined in all sub-types.
 *
 * <p>
 * When auto-generation of properties is enabled, use of this annotation is not required unless you need to override
 * the defaults; see {@link JSimpleClass#autogenFields}.
 *
 * <p><b>Non-Reference Fields</b></p>
 *
 * <p>
 * If the field is not a reference field, the property type is inferred from the type of the annotated method or,
 * in the case of complex sub-fields, the generic type of the collection class. The name of the property type
 * must be registered in the {@link org.jsimpledb.core.FieldTypeRegistry} (perhaps via {@link JFieldType &#64;JFieldType}),
 * and the corresponding {@link org.jsimpledb.core.FieldType} is then used to encode/decode field values.
 * The type name may also be specified explicitly by {@link #name}.
 *
 * <p>
 * Simple fields may be {@link #indexed}; see {@link org.jsimpledb.index} for information on querying indexes.
 * {@link org.jsimpledb.Counter} fields may not be indexed.
 *
 * <p><b>Reference Fields</b></p>
 *
 * <p>
 * If the type of the field is (assignable to) a {@link JSimpleClass &#64;JsimpleClass}-annotated Java model object type,
 * then the field is a reference field.
 *
 * <p>
 * Reference fields are always indexed; the value of {@link #indexed} is ignored.
 *
 * <p><b>Copy Cascades</b></p>
 *
 * <p>
 * The {@link org.jsimpledb.JObject} methods {@link org.jsimpledb.JObject#cascadeCopyIn cascadeCopyIn()},
 * {@link org.jsimpledb.JObject#cascadeCopyOut cascadeCopyOut()}, and {@link org.jsimpledb.JObject#cascadeCopyTo cascadeCopyTo()}
 * copy a graph of related objects between transactions by first copying a starting object, then cascading through matching
 * reference fields and repeating recursively. This cascade operation is capable of traversing references in both the
 * forward and inverse directions.
 *
 * <p>
 * Which reference fields are traversed in a particular copy operation is determined by the supplied <i>cascade name</i>.
 * Outgoing references are traversed if the cascade name is in the reference field's {@link #cascades} property,
 * while incoming references from other objects are traversed (in the reverse direction) if the cascade name is in the
 * referring object's reference field's {@link #inverseCascades}.
 *
 * <p>
 * For example:
 * <pre>
 *  &#64;JSimpleClass
 *  public interface TreeNode extends JObject {
 *
 *      /**
 *       * Get the parent of this node, or null if node is a root.
 *       *&#47;
 *      &#64;JField(<b>cascades = { "tree", "ancestors" }</b>, <b>inverseCascades = { "tree", "descendants" }</b>)
 *      TreeNode getParent();
 *      void setParent(TreeNode parent);
 *
 *      /**
 *       * Get the children of this node.
 *       *&#47;
 *      &#64;FollowPath(inverseOf = "parent", startingFrom = TreeNode.class)
 *      NavigableSet&lt;TreeNode&gt; getChildren();
 *
 *      default TreeNode copySubtreeTo(JTransaction dest) {
 *          return (TreeNode)this.cascadeCopyTo(dest, <b>"descendants"</b>, false);
 *      }
 *
 *      default TreeNode copyWithAnscestorsTo(JTransaction dest) {
 *          return (TreeNode)this.cascadeCopyTo(dest, <b>"ancestors"</b>, false);
 *      }
 *
 *      default TreeNode copyEntireTreeTo(JTransaction dest) {
 *          return (TreeNode)this.cascadeCopyTo(dest, <b>"tree"</b>, false);
 *      }
 *
 *      default TreeNode cloneEntireTreeTo(JTransaction dest) {
 *          return (TreeNode)this.cascadeCopyTo(dest, <b>"tree"</b>, true);
 *      }
 *
 *      default TreeNode cloneEntireTree() {
 *          return (TreeNode)this.cascadeCopyTo(this.getTransaction(), <b>"tree"</b>, true);
 *      }
 *  }
 * </pre>
 *
 * <p><b>Delete Cascades</b></p>
 *
 * <p>
 * Reference fields have configurable behavior when the referring object or the referred-to object is deleted;
 * see {@link #onDelete} and {@link #cascadeDelete}.
 *
 * <p><b>Uniqueness Constraints</b></p>
 *
 * <p>
 * Fields that are not complex sub-fields may be marked as {@link #unique} to impose a uniqueness constraint on the value.
 * Fields with uniqueness constraints must be indexed. Uniqueness constraints are handled at the JSimpleDB layer and function as
 * an implicit validation constraint. In other words, the constraint is verified when the validation queue is processed
 * and is affected by the transaction's configured {@link org.jsimpledb.ValidationMode}.
 *
 * <p>
 * Optionally, specific field values may be marked as excluded from the uniqueness constraint via {@link #uniqueExclude}.
 * If so, the specified values may appear in multiple objects without violating the constraint. Because null values
 * are not allowed in annotations, include {@link #NULL} to indicate that null values should be excluded.
 *
 * <p>
 * In {@link org.jsimpledb.ValidationMode#AUTOMATIC}, any upgraded {@link org.jsimpledb.JObject}s are automatically
 * added to the validation queue, so a uniqueness constraint added in a new schema version will be automatically verified
 * when any object is upgraded.
 *
 * <p>
 * Beware however, that like all other types of validation constraint, uniqueness constraints can be added or changed on a field
 * without any schema version change. Therefore, after such changes, it's possible for pre-existing database objects that were
 * previously valid to suddenly become invalid, and these invalid objects would not be detected until they are validated in some
 * future transaction and a validation exception is thrown.
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
public @interface JField {

    /**
     * Value for use with {@link #uniqueExclude} to represent a null value.
     */
    String NULL = "\u0000";     // note: this is non-conflicting because \u0000 is invalid in return from FieldType.toString(T)

    /**
     * The name of this field.
     *
     * <p>
     * If equal to the empty string (default value), the name is inferred from the name of the annotated Java bean getter method.
     *
     * <p>
     * For sub-fields of complex fields, this property must be left unset.
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
     *
     * <p>
     * For reference fields (i.e., methods with return value equal to a {@link JSimpleClass &#64;JSimpleClass}-annotated class),
     * this property must be left unset.
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
     * @see org.jsimpledb.core.FieldType
     * @see org.jsimpledb.core.FieldTypeRegistry#getFieldType(String, long)
     */
    String type() default "";

    /**
     * Optional override for the {@linkplain org.jsimpledb.core.FieldType#getEncodingSignature encoding signature}
     * associated with this field's {@link org.jsimpledb.core.FieldType} used to encode/decode field values.
     *
     * @return the encoding signature of the field's type
     * @see org.jsimpledb.core.FieldType
     * @see org.jsimpledb.core.FieldTypeRegistry#getFieldType(String, long)
     */
    long typeSignature() default 0;

    /**
     * Storage ID for this field.
     *
     * <p>
     * Value should be positive and unique within the contained class.
     *
     * <p>
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
     * a composite index on multiple fields, use {@link JCompositeIndex}.
     *
     * <p>
     * Note: reference fields are always indexed (for reference fields, this property is ignored).
     *
     * @return whether the field is indexed
     * @see JCompositeIndex
     */
    boolean indexed() default false;

    /**
     * Define forward copy cascades for the annotated reference field.
     *
     * <p>
     * When {@link org.jsimpledb.JObject#cascadeCopyIn JObject.cascadeCopyIn()},
     * {@link org.jsimpledb.JObject#cascadeCopyOut JObject.cascadeCopyOut()}, or
     * {@link org.jsimpledb.JObject#cascadeCopyTo JObject.cascadeCopyTo()} is invoked, if the given cascade name is one
     * of the names listed here, and an object with the annotated reference field is copied, then the reference field will
     * will be traversed in the forward direction and the referred-to object will also be copied.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     *
     * @return whether the field is indexed
     * @see org.jsimpledb.JObject#cascadeCopyTo JObject.cascadeCopyTo()
     * @see org.jsimpledb.JTransaction#cascadeFindAll JTransaction.cascadeFindAll()
     */
    String[] cascades() default {};

    /**
     * Define inverse copy cascades for the annotated reference field.
     *
     * <p>
     * When {@link org.jsimpledb.JObject#cascadeCopyIn JObject.cascadeCopyIn()},
     * {@link org.jsimpledb.JObject#cascadeCopyOut JObject.cascadeCopyOut()}, or
     * {@link org.jsimpledb.JObject#cascadeCopyTo JObject.cascadeCopyTo()} is invoked, if the given cascade name is one
     * of the names listed here, and an object with the annotated reference field refers to an object that is copied, then the
     * reference field will be traversed in the inverse direction and the referring object will also be copied.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     *
     * @return whether the field is indexed
     * @see org.jsimpledb.JObject#cascadeCopyTo JObject.cascadeCopyTo()
     * @see org.jsimpledb.JTransaction#cascadeFindAll JTransaction.cascadeFindAll()
     */
    String[] inverseCascades() default {};

    /**
     * For reference fields, configure the behavior when the referred-to object is
     * {@linkplain org.jsimpledb.JObject#delete deleted}.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     *
     * @return desired behavior when a referenced object is deleted
     * @see #cascadeDelete
     * @see org.jsimpledb.JObject#delete
     */
    DeleteAction onDelete() default DeleteAction.EXCEPTION;

    /**
     * For reference fields, configure cascading behavior when the referring object is
     * {@linkplain org.jsimpledb.JObject#delete deleted}. If set to true, the referred-to object
     * is automatically deleted as well.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
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
     * This property creates an implicit uniqueness validation constraint.
     *
     * <p>
     * The constraint will be checked any time normal validation is performed on an object containing the field.
     * More precisely, a uniqueness constraint behaves like a JSR 303
     * validation constraint with {@code groups() = }<code>{ </code>{@link javax.validation.groups.Default}{@code .class,
     * }{@link org.jsimpledb.UniquenessConstraints}{@code .class}<code> }</code>. Therefore, uniqueness constraints
     * are included in default validation, but you can also validate <i>only</i> uniqueness constraints via
     * {@link org.jsimpledb.JObject#revalidate myobj.revalidate(UniquenessConstraints.class)}.
     *
     * <p>
     * This property must be false for sub-fields of complex fields, and for any field that is not indexed.
     *
     * @return whether the field's value should be unique
     * @see #uniqueExclude
     * @see org.jsimpledb.UniquenessConstraints
     */
    boolean unique() default false;

    /**
     * Specify field value(s) which are excluded from the uniqueness constraint.
     *
     * <p>
     * The specified values must be valid {@link String} encodings of the associated field, or {@link #NULL}
     * to indicate null values. For example:
     * <pre>
     *  &#64;JField(indexed = true, unique = true, uniqueExclude = { "Infinity", "-Infinity" })
     *  public abstract float getPriority();
     *
     *  &#64;JField(indexed = true, unique = true, uniqueExclude = { JField.NULL })
     *  public abstract String getName();
     * </pre>
     *
     * <p>
     * This property must be left empty when {@link #unique} is false.
     *
     * @return values to exclude from the uniqueness constraint
     * @see #unique
     */
    String[] uniqueExclude() default {};

    /**
     * Allow assignment to deleted objects in normal transactions.
     *
     * <p>
     * For non-reference fields, this property must be equal to its default value.
     *
     * <p>
     * For reference fields, when true this property prevents setting this field to reference a deleted object,
     * causing a {@link org.jsimpledb.core.DeletedObjectException} to be thrown instead. Used together with
     * {@link DeleteAction#EXCEPTION} (see {@link #onDelete}), this guarantees this field won't contain any dangling references.
     *
     * <p>
     * This property only controls validation in regular (non-snapshot transactions); {@link #allowDeletedSnapshot}
     * separately controls validation for {@link org.jsimpledb.SnapshotJTransaction}s.
     *
     * <p>
     * For consistency, this property must be set to true when {@link #onDelete} is set to {@link DeleteAction#NOTHING}.
     *
     * @return whether the reference field should allow assignment to deleted objects in normal transactions
     * @see #onDelete
     * @see #allowDeletedSnapshot
     * @see JSimpleClass#autogenAllowDeleted
     */
    boolean allowDeleted() default false;

    /**
     * Allow assignment to deleted objects in snapshot transactions.
     *
     * <p>
     * For non-reference fields, this property must be equal to its default value.
     *
     * <p>
     * This property is equivalent to {@link #allowDeleted}, but applies to {@link org.jsimpledb.SnapshotJTransaction}s
     * instead of normal {@link org.jsimpledb.JTransaction}s; see {@link #allowDeleted} for details.
     *
     * <p>
     * Snapshot transactions typically hold a copy of some small portion of the database. If this property is set to false,
     * then it effectively creates a requirement that this "small portion" be transitively closed under object references.
     *
     * <p>
     * For consistency, this property must be set to true when {@link #onDelete} is set to {@link DeleteAction#NOTHING}.
     *
     * @return whether the reference field should allow assignment to deleted objects in snapshot transactions
     * @see #onDelete
     * @see #allowDeleted
     * @see JSimpleClass#autogenAllowDeletedSnapshot
     */
    boolean allowDeletedSnapshot() default true;

    /**
     * Specify the {@link UpgradeConversionPolicy} policy to apply when a schema change occurs and this field's type changes.
     *
     * <p>
     * With one restriction<sup>*</sup>, JSimpleDB supports schema changes that alter a field's type, and in some cases
     * can automatically convert field values from the old to the new type (for example, from the {@code int} value {@code 1234}
     * to the {@link String} value {@code "1234"}).
     *
     * <p>
     * See {@link org.jsimpledb.core.FieldType#convert} for details about conversions between simple field types. In addition,
     * {@link org.jsimpledb.Counter} fields can be converted to/from any numeric Java primitive (or primitive wrapper) type.
     *
     * <p>
     * This property defines the {@link UpgradeConversionPolicy} for the annotated field when upgrading an object from some
     * other schema version to the current schema version. Note custom conversion logic is also possible using
     * {@link OnVersionChange &#64;OnVersionChange} methods.
     *
     * <p>
     * For sub-fields of complex fields, this property is ignored.
     *
     * <p>
     * <sup>*</sup>A simple field may not have different types across schema versions and be indexed in both versions.
     *
     * @return upgrade conversion policy for this field
     * @see UpgradeConversionPolicy
     * @see org.jsimpledb.core.FieldType#convert FieldType.convert()
     */
    UpgradeConversionPolicy upgradeConversion() default UpgradeConversionPolicy.ATTEMPT;
}

