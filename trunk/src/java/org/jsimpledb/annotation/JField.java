
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

import org.jsimpledb.DeleteAction;

/**
 * Java annotation for simple fields, including reference fields that refer to other Java model object types.
 *
 * <p>
 * This annotation is used in two ways:
 * <ul>
 *  <li>To annotate the getter method of a Java bean property in a Java model object class</li>
 *  <li>To define the sub-field(s) of a complex set, list, or map field
 *      (i.e., the collection element or map key and value types)</li>
 * </ul>
 * </p>
 *
 * <p>
 * If the field is a reference field, the property type must be (a super-type of) the Java model object type
 * to which it refers.
 * </p>
 *
 * <p>
 * If the field is not a reference field, the property type, which can either be specified by {@link #name}
 * or inferred from the annotated method, must be supported by a some {@link org.jsimpledb.FieldType}
 * registered in the {@link org.jsimpledb.FieldTypeRegistry} (perhaps via {@link JFieldType &#64;JFieldType}).
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
     * If set, this must equal the name of a type registered in the {@link org.jsimpledb.FieldTypeRegistry}
     * associated with the {@link org.jsimpledb.JSimpleDB} instance, and the annotated method's return type
     * must match the {@link org.jsimpledb.FieldType}'s
     * {@linkplain org.jsimpledb.FieldType#getTypeToken supported Java type}.
     *
     * <p>
     * If equal to the empty string (default value), then the Java type is inferred from the return type of the getter method
     * and the {@link org.jsimpledb.FieldType} is found via
     * {@link org.jsimpledb.FieldTypeRegistry#getFieldType(com.google.common.reflect.TypeToken)
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
     *     &#64;JListField(storageId = 11,
     *       element = &#64;JField(storageId = 12, <b>type = "float"</b>)) // nulls will be disallowed
     *     public abstract List&lt;<b>Float</b>&gt; getScores();
     * }
     * </pre>
     * </p>
     */
    String type() default "";

    /**
     * Storage ID for this field. Value must be positive and unique within the contained class.
     */
    int storageId();

    /**
     * Whether this field is indexed or not.
     *
     * <p>
     * Note: reference fields are always indexed; for reference fields, this property is ignored.
     * </p>
     */
    boolean indexed() default false;

    /**
     * For reference fields, configure the behavior when a referred-to object is
     * {@linkplain org.jsimpledb.jlayer.JObject#delete deleted}.
     *
     * <p>
     * This field is ignored for non-reference fields.
     * </p>
     */
    DeleteAction onDelete() default DeleteAction.EXCEPTION;
}

