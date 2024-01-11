
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.PermazenObject;
import io.permazen.UniquenessConstraints;
import io.permazen.encoding.Encoding;

import jakarta.validation.groups.Default;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation that defines a composite index.
 *
 * <p>
 * A composite index is an index on two or more fields (to define a single-field index, see {@link PermazenField#indexed}.
 * All fields participating in a composite index must be (a) simple and (b) not a sub-field of a complex field.
 *
 * <p>
 * The class or interface does not have to be {@link PermazenType &#64;PermazenType}-annotated; annotations
 * are "inherited" and so apply to all {@link PermazenType &#64;PermazenType}-annotated sub-types.
 *
 * <p>
 * {@link PermazenCompositeIndex &#64;PermazenCompositeIndex} is a {@linkplain Repeatable repeatable annotation}.
 *
 * <p><b>Uniqueness Constraints</b></p>
 *
 * Uniqueness constraints are supported, and their enforcement is handled in the same way as for uniqueness constraints
 * on simply indexed fields (see {@link PermazenField#unique &#64;PermazenField.unique()}). A uniqueness constriant on a composite
 * index means each combination of field values must be unique to a single object. Specific field value combinations may be
 * excluded from the uniqueness constraint by specifying the corresponding comma-separated {@link String}-encoded tuples
 * in {@link #uniqueExclude}.
 *
 * @see PermazenType
 * @see PermazenField#indexed
 */
@Repeatable(PermazenCompositeIndexes.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.TYPE })
@Documented
public @interface PermazenCompositeIndex {

    /**
     * The name of this index.
     *
     * <p>
     * The name must be unique among all other indexes and simple fields in a Java model type.
     *
     * @return the index name
     */
    String name();

    /**
     * Storage ID for this index.
     *
     * <p>
     * Normally this value is left as zero, in which case a value will be automatically assigned.
     *
     * <p>
     * Otherwise, the value should be positive and unique within the schema.
     *
     * @return this index's storage ID, or zero for automatic assignment
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
     * validation constraint with {@code groups() = }<code>{ </code>{@link Default}{@code .class,
     * }{@link UniquenessConstraints}{@code .class}<code> }</code>. Therefore, uniqueness constraints
     * are included in default validation, but you can also validate <i>only</i> uniqueness constraints via
     * {@link PermazenObject#revalidate myobj.revalidate(UniquenessConstraints.class)}.
     *
     * @return whether the composite index's field values should be unique
     * @see #uniqueExclude
     * @see UniquenessConstraints
     */
    boolean unique() default false;

    /**
     * Specify field value combination(s) to be excluded from the uniqueness constraint.
     *
     * <p>
     * The specified values must {@link String}s containing comma-separated encodings of the associated fields.
     *
     * <p>
     * Unlike the field values used with {@link PermazenField#uniqueExclude &#64;PermazenField.uniqueExclude()}, the field values
     * contained here are the {@linkplain Encoding#toParseableString self-delimiting string forms}.
     *
     * <p>
     * For some encodings this makes a difference: e.g., {@link String} values must be surrounded by double quotes.
     * For example:
     *
     * <pre>
     * &#64;PermazenType(compositeIndexes =
     *  &#64;PermazenCompositeIndex(name = "firstAndLast", fields = { "lastName", firstName" },
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
     * To specify a null value in the list, specify the {@linkplain Encoding#toParseableString
     * self-delimiting string form} of the null value (this is almost always the string {@code "null"}):
     *
     * <pre>
     * &#64;PermazenType(compositeIndexes =
     *  &#64;PermazenCompositeIndex(name = "firstAndLast", fields = { "lastName", firstName" },
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
