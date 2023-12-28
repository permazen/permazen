
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import com.google.common.reflect.TypeToken;

import io.permazen.Counter;
import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.UniquenessConstraints;
import io.permazen.UpgradeConversionPolicy;
import io.permazen.ValidationMode;
import io.permazen.core.Database;
import io.permazen.core.DeleteAction;
import io.permazen.core.DeletedObjectException;
import io.permazen.encoding.DefaultEncodingRegistry;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingId;
import io.permazen.encoding.EncodingRegistry;

import jakarta.validation.groups.Default;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for defining simple fields (including reference fields that refer to other Java model object types)
 * and {@link Counter} fields.
 *
 * <p>
 * This annotation is used in two scenarios:
 * <ul>
 *  <li>To configure a <b>simple</b> or <b>counter</b> database field, by annotating the corresponding abstract Java bean
 *      property "getter" method</li>
 *  <li>To configure the <b>sub-field</b> of a <b>complex</b> database field (i.e., set, list, or map), that is,
 *      a collection {@code element} field, or a map {@code key} or {@code value} field. In this case this annotation
 *      nests within the corresponding {@link JListField &#64;JListField}, {@link JSetField &#64;JSetField},
 *      or {@link JMapField &#64;JMapField} annotation.</li>
 * </ul>
 *
 * <p>
 * When auto-generation of properties is enabled, use of this annotation is not required unless you want to override
 * the defaults; see {@link PermazenType#autogenFields}.
 *
 * <p>
 * The annotated method's declaring class does not have to be a {@link PermazenType &#64;PermazenType}-annotated type;
 * annotations are "inherited" and so apply to all {@link PermazenType &#64;PermazenType}-annotated sub-types.
 *
 * <p><b>Reference Fields</b></p>
 *
 * <p>
 * If the type of the field is a {@link PermazenType &#64;PermazenType}-annotated Java model object type, or any supertype
 * thereof, then the field is a <b>reference</b> field. Reference fields are simple fields that refer to other database objects.
 *
 * <p><b>Non-Reference Fields</b></p>
 *
 * <p>
 * If the field is not a reference field, the field's Java type is inferred from the type of the annotated method or,
 * in the case of complex sub-fields, the generic type of the collection class. The field's Java type must be supported by
 * some {@link Encoding} registered in the {@link EncodingRegistry} that is
 * {@linkplain io.permazen.PermazenConfig.Builder configured} for the database so that the field's values can be encoded
 * into {@code byte[]} array values in the key/value store.
 *
 * <p>
 * See {@link DefaultEncodingRegistry} for a list of built-in (pre-defined) encodings.
 *
 * <p>
 * To use user-defined encodings, configure a custom {@link EncodingRegistry} and specify them via {@link #encoding}.
 *
 * <p><b>Referential Integrity</b></p>
 *
 * <p>
 * In general, reference fields may reference objects that don't actually exist. This can happen in one of two ways:
 * (a) a field is set to an invalid reference, or (b) a field references a valid object that is subsequently deleted.
 * The {@link #allowDeleted} and {@link #inverseDelete} properties, respectively, control whether (a) or (b) is permitted.
 *
 * <p>
 * By default, neither (a) nor (b) is allowed; if attempted, a {@link DeletedObjectException} is thrown.
 * This ensures references are always valid.
 *
 * <p><b>Indexing</b></p>
 *
 * <p>
 * Simple fields may be indexed by marking them as {@link #indexed()}; see {@link JTransaction} for information on querying
 * indexes. This includes reference fields and fields with user-defined custom encodings.
 *
 * <p>
 * Reference fields are always indexed.
 *
 * <p>
 * {@link Counter} fields may not be indexed.
 *
 * <p>
 * Two or more simple fields may be indexed together in a composite index; see {@link JCompositeIndex &#64;JCompositeIndex}.
 *
 * <p><b>Cascades</b></p>
 *
 * <p>
 * The {@link JObject} methods {@link JObject#copyIn copyIn()},
 * {@link JObject#copyOut copyOut()}, and {@link JObject#copyTo copyTo()}
 * copy a graph of related objects between transactions by first copying a starting object, then cascading through matching
 * reference fields and repeating recursively. This cascade operation is capable of traversing references in both the
 * forward and inverse directions. There is also {@link JTransaction#cascade} which performs a general purpose
 * cascade exploration.
 *
 * <p>
 * Which reference fields are traversed in a particular find or copy operation is determined by the supplied <i>cascade name</i>.
 * Outgoing references are traversed if the cascade name is in the reference field's {@link #forwardCascades} property,
 * while incoming references from other objects are traversed (in the reverse direction) if the cascade name is in the
 * referring object's reference field's {@link #inverseCascades}.
 *
 * <p>
 * For example:
 * <pre>
 *  &#64;PermazenType
 *  public interface TreeNode extends JObject {
 *
 *      /**
 *       * Get the parent of this node, or null if node is a root.
 *       *&#47;
 *      &#64;JField(<b>forwardCascades = { "tree", "ancestors" }</b>, <b>inverseCascades = { "tree", "descendants" }</b>)
 *      TreeNode getParent();
 *      void setParent(TreeNode parent);
 *
 *      /**
 *       * Get the children of this node.
 *       *&#47;
 *      &#64;FollowPath("&lt;-TreeNode.parent")
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
 * see {@link #inverseDelete} and {@link #forwardDelete}.
 *
 * <p><b>Uniqueness Constraints</b></p>
 *
 * <p>
 * Simple fields that are not complex sub-fields may be marked as {@link #unique} to impose a uniqueness constraint on the field.
 * Uniqueness constraints function as an implicit validation constraint. In other words, the uniqueness constraint is verified
 * when the validation queue is processed, and is affected by the transaction's configured {@link ValidationMode}.
 *
 * <p>
 * Optionally, specific values may be marked as excluded from the uniqueness constraint via {@link #uniqueExclude}.
 * If so, the specified values may appear in more than one object without violating the constraint. Because it's not possible
 * to specify null in annotations, specify {@link #NULL} to indicate that null values should be excluded.
 *
 * <p>
 * In {@link ValidationMode#AUTOMATIC}, any upgraded {@link JObject}s are automatically added to the validation queue,
 * so a uniqueness constraint added in a new schema will be automatically verified when any object is upgraded.
 *
 * <p>
 * Beware however, that like all other types of validation constraint, uniqueness constraints can be changed without requiring
 * a schema change (e.g., by adding {@code &#64;NotNull} to an existing property). Therefore, after such changes, it's possible
 * for pre-existing database objects that were previously valid to suddenly become invalid, and these invalid objects will not
 * be detected until they are validated in some future transaction and a validation exception thrown.
 *
 * <p><b>Upgrade Conversions</b></p>
 *
 * <p>
 * When a field's type has changed in a new schema, the old field value can be automatically converted into the
 * new type. See {@link #upgradeConversion} for how to control this behavior.
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
     *
     * <p>
     * Note: this particular {@link String} will never conflict with any actual field values because it contains a character
     * that is not allowed in the return value from {@link Encoding#toString(Object) Encoding.toString()}.
     */
    String NULL = "\u0000";

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
     * Specify the encoding for this field by {@link EncodingId} URN.
     *
     * <p>
     * If set, this must equal the {@link EncodingId} of an {@link Encoding} registered in the {@link EncodingRegistry}
     * associated with the {@link Database} instance, and the annotated method's return type must match the
     * {@link Encoding}'s {@linkplain Encoding#getTypeToken supported Java type}.
     *
     * <p>
     * If this is left unset (empty string), then the Java type is inferred from the return type of the getter method
     * and the {@link Encoding} is found via {@link EncodingRegistry#getEncoding(TypeToken)}.
     *
     * <p>
     * For any of Permazen's built-in types, the Permazen URN prefix {@value io.permazen.encoding.EncodingIds#PERMAZEN_PREFIX}
     * may be omitted. Otherwise, see {@link EncodingId} for the required format. Custom encodings can be found automatically
     * on the application class path; see {@link DefaultEncodingRegistry} for details.
     *
     * <p>
     * For reference fields (i.e., methods with return value equal to a {@link PermazenType &#64;PermazenType}-annotated class),
     * this property must be left unset.
     *
     * <p>
     * For sub-fields of complex fields, this property can be used to force a primitive type instead of a
     * primitive wrapper type. In that case, the complex field will disallow null values. For example:
     * <pre>
     *  &#64;JSetField(element = &#64;JField(<b>type = "float"</b>)) // nulls will be disallowed
     *  public abstract List&lt;<b>Float</b>&gt; getScores();
     * </pre>
     *
     * @return URN identifying the field's encoding
     * @see Encoding
     * @see EncodingRegistry#getEncoding(EncodingId)
     */
    String encoding() default "";

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
     * Whether this field is indexed or not.
     *
     * <p>
     * Setting this property to true creates a simple index on this field. To have this field participate in
     * a composite index on multiple fields, use {@link JCompositeIndex &#64;JCompositeIndex}.
     *
     * <p>
     * Note: reference fields are always indexed (for reference fields, this property is ignored).
     *
     * @return whether the field is indexed
     * @see JCompositeIndex &#64;JCompositeIndex
     */
    boolean indexed() default false;

    /**
     * Define forward find/copy cascades for the annotated reference field.
     *
     * <p>
     * When doing a find or copy cascade operation, if the cascade name is one of the names listed here,
     * and an object with the annotated reference field is copied, then the annotated reference field will
     * will be traversed in the forward direction.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     *
     * @return forward cascade names for the annotated reference field
     * @see io.permazen.JObject#copyTo JObject.copyTo()
     * @see io.permazen.JTransaction#cascade JTransaction.cascade()
     */
    String[] forwardCascades() default {};

    /**
     * Define inverse find/copy cascades for the annotated reference field.
     *
     * <p>
     * When doing a find or copy cascade operation, if the cascade name is one of the names listed here,
     * and an object with the annotated reference field is copied, then the annotated reference field will
     * will be traversed in the inverse direction.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     *
     * @return inverse cascade names for the annotated reference field
     * @see io.permazen.JObject#copyTo JObject.copyTo()
     * @see io.permazen.JTransaction#cascade JTransaction.cascade()
     */
    String[] inverseCascades() default {};

    /**
     * For reference fields, configure the behavior when the referred-to object is
     * {@linkplain JObject#delete deleted}.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     *
     * @return desired behavior when a referenced object is deleted
     * @see #forwardDelete
     * @see io.permazen.JObject#delete
     */
    DeleteAction inverseDelete() default DeleteAction.EXCEPTION;

    /**
     * For reference fields, configure cascading behavior when the referring object is
     * {@linkplain JObject#delete deleted}. If set to true, the referred-to object
     * is automatically deleted as well.
     *
     * <p>
     * For non-reference fields this property must be equal to its default value.
     *
     * @return whether deletion should cascade to the referred-to object
     * @see #inverseDelete
     * @see JObject#delete
     */
    boolean forwardDelete() default false;

    /**
     * Require this field's value to be unique among all database objects.
     *
     * <p>
     * This property creates an implicit uniqueness validation constraint.
     *
     * <p>
     * The constraint will be checked any time normal validation is performed on an object containing the field.
     * More precisely, a uniqueness constraint behaves like a JSR 303
     * validation constraint with {@code groups() = }<code>{ </code>{@link Default}{@code .class,
     * }{@link UniquenessConstraints}{@code .class}<code> }</code>. Therefore, uniqueness constraints
     * are included in default validation, but you can also validate <i>only</i> uniqueness constraints via
     * {@link JObject#revalidate myobj.revalidate(UniquenessConstraints.class)}.
     *
     * <p>
     * This property must be false for sub-fields of complex fields, and for any field that is not indexed.
     *
     * @return whether the field's value should be unique
     * @see #uniqueExclude
     * @see io.permazen.UniquenessConstraints
     */
    boolean unique() default false;

    /**
     * Specify field value(s) which are excluded from the uniqueness constraint.
     *
     * <p>
     * The specified values must be valid {@link String} encodings of the associated field (as returned by
     * {@link Encoding#toString(Object) Encoding.toString(T)}), or the constant {@link #NULL}
     * to indicate a null value. For example:
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
     * Allow the field to reference non-existent objects in normal transactions.
     *
     * <p>
     * For non-reference fields, this property must be equal to its default value.
     *
     * <p>
     * Otherwise, if this property is set to false, the field is disallowed from ever referring to a non-existent object;
     * instead, a {@link DeletedObjectException} will be thrown. When used together with
     * {@link DeleteAction#EXCEPTION} (see {@link #inverseDelete}), the field is guaranteed to never be a dangling reference.
     *
     * <p>
     * This property only applies to regular (non-detached) transactions.
     *
     * <p>
     * For consistency, this property must be set to true when {@link #inverseDelete} is set to {@link DeleteAction#IGNORE}.
     *
     * @return whether the reference field should allow assignment to deleted objects in normal transactions
     * @see #inverseDelete
     * @see PermazenType#autogenAllowDeleted
     */
    boolean allowDeleted() default false;

    /**
     * Specify the {@link UpgradeConversionPolicy} policy to apply when this field's type has changed due to a schema change.
     *
     * <p>
     * Permazen supports schema changes that alter a field's type, and in some cases can automatically convert field values
     * from the old to the new type (for example, from the {@code int} value {@code 1234} to the {@link String} value
     * {@code "1234"}).
     *
     * <p>
     * See {@link Encoding#convert Encoding.convert()} for details about conversions between simple encodings.
     * In addition, {@link Counter} fields can be converted to/from any numeric Java primitive (or primitive wrapper)
     * type.
     *
     * <p>
     * This property defines the {@link UpgradeConversionPolicy} for the annotated field when upgrading an object from some
     * other schema to the current schema. Note custom conversion logic is also possible using
     * {@link OnSchemaChange &#64;OnSchemaChange} methods.
     *
     * <p>
     * For sub-fields of complex fields, this property is ignored.
     *
     * @return upgrade conversion policy for this field
     * @see UpgradeConversionPolicy
     * @see io.permazen.encoding.Encoding#convert Encoding.convert()
     * @see OnSchemaChange
     */
    UpgradeConversionPolicy upgradeConversion() default UpgradeConversionPolicy.ATTEMPT;
}
