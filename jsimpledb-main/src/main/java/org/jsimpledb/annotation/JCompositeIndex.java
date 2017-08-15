
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation defining a composite index on all {@link JSimpleClass}-annotated sub-types.
 *
 * <p>
 * A composite index is an index on two or more fields (to define a single-field index,
 * just set {@link JField#indexed} to true). All fields indexed in a composite index
 * must be (a) simple and (b) not a sub-field of a complex field.
 *
 * <p>
 * {@link JCompositeIndex &#64;JCompositeIndex} is a {@linkplain Repeatable repeatable annotation}. Instances
 * may annotate any {@link JSimpleClass &#64;JSimpleClass}-annotated class or supertype thereof. Put another way,
 * instances of a {@link JSimpleClass &#64;JSimpleClass}-annotated class that is a subtype of a
 * {@link JCompositeIndex &#64;JCompositeIndex}-annotated type will be included in the corresponding composite index.
 *
 * <p><b>Uniqueness Constraints</b></p>
 *
 * Uniqueness constraints are supported, and their enforcement is handled in the same way as for uniqueness constraints
 * on simply indexed fields (see {@link JField#unique &#64;JField.unique()}). A uniqueness constriant on a composite
 * index means each object's combination of field values must be unique. Specific field value combinations may be
 * excluded from the uniqueness constraint by specifying the corresponding comma-separated {@link String} encoded tuples
 * in {@link #uniqueExclude}.
 *
 * @see JSimpleClass
 * @see JField#indexed
 */
@Repeatable(JCompositeIndexes.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.TYPE })
@Documented
public @interface JCompositeIndex {

    /**
     * The name of this composite index. Must be unique with respect to any indexed Java model type.
     *
     * @return the index name
     */
    String name();

    /**
     * The storage ID for this composite index. Value should be positive; if zero, the configured
     * {@link org.jsimpledb.StorageIdGenerator} will be consulted to auto-generate a value.
     *
     * @return the index storage ID
     * @see org.jsimpledb.StorageIdGenerator#generateCompositeIndexStorageId StorageIdGenerator.generateCompositeIndexStorageId()
     */
    int storageId() default 0;

    /**
     * The names of the indexed fields, in the desired order. At least two fields must be specified.
     *
     * @return the names of the indexed fields
     */
    String[] fields();

    /**
     * Require each object's field combination to be unique among all types to which this annotation applies.
     *
     * <p>
     * This property creates an implicit uniqueness validation constraint.
     *
     * <p>
     * The constraint will be checked any time normal validation is performed on an object.
     * More precisely, a uniqueness constraint behaves like a JSR 303
     * validation constraint with {@code groups() = }<code>{ </code>{@link javax.validation.groups.Default}{@code .class,
     * }{@link org.jsimpledb.UniquenessConstraints}{@code .class}<code> }</code>. Therefore, uniqueness constraints
     * are included in default validation, but you can also validate <i>only</i> uniqueness constraints via
     * {@link org.jsimpledb.JObject#revalidate myobj.revalidate(UniquenessConstraints.class)}.
     *
     * @return whether the composite index's field values should be unique
     * @see #uniqueExclude
     * @see org.jsimpledb.UniquenessConstraints
     */
    boolean unique() default false;

    /**
     * Specify field value combination(s) to be excluded from the uniqueness constraint.
     *
     * <p>
     * The specified values must {@link String}s containing comma-separated encodings of the associated fields.
     *
     * <p>
     * Unlike the field values used with {@link JField#uniqueExclude &#64;JField.uniqueExclude()}, the field values
     * contained here are the {@linkplain org.jsimpledb.core.FieldType#toParseableString self-delimiting string forms}.
     *
     * <p>
     * For some field types this makes a difference: e.g., {@link String} values must be surrounded by double quotes.
     * For example:
     *
     * <pre>
     * &#64;JSimpleClass(compositeIndexes =
     *  &#64;JCompositeIndex(name = "firstAndLast", fields = { "lastName", firstName" },
     *                      <b>unique = true, uniqueExclude = "\"Unknown\", \"Unknown\""</b>))
     *  public abstract class Person {
     *
     *      public abstract String getLastName();
     *      public abstract void setLastName(String x);
     *
     *      public abstract String getFirstName();
     *      public abstract void setFirstName(String x);
     *  }
     * </pre>
     *
     * <p>
     * To specify a null value in the list, specify the {@linkplain org.jsimpledb.core.FieldType#toParseableString
     * self-delimiting string form} of the null value (this is almost always the string {@code "null"}):
     *
     * <pre>
     * &#64;JSimpleClass(compositeIndexes =
     *  &#64;JCompositeIndex(name = "firstAndLast", fields = { "lastName", firstName" },
     *                      <b>unique = true, uniqueExclude = "null, null"</b>))
     *  public abstract class Person {
     *
     *      public abstract String getLastName();
     *      public abstract void setLastName(String x);
     *
     *      public abstract String getFirstName();
     *      public abstract void setFirstName(String x);
     *  }
     * </pre>
     *
     * <p>
     * This property must be left empty when {@link #unique} is false.
     *
     * @return value combinations to exclude from the uniqueness constraint
     * @see #unique
     */
    String[] uniqueExclude() default {};
}

