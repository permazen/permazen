
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
 * Annotation for methods that are to be invoked whenever an object's schema version has just changed.
 *
 * <p><b>Method Parameters</b></p>
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and
 * take one, two, or all three of the following parameters in order:
 * <ol>
 * <li>{@code int oldVersion} - previous schema version; should be present only if {@link #oldVersion} is zero</li>
 * <li>{@code int newVersion} - new schema version; should be present only if {@link #newVersion} is zero</li>
 * <li>{@code Map<Integer, Object> oldValues} <i>or</i> {@code Map<String, Object> oldValues} - immutable map containing
 *      all field values from the previous version of the object, indexed by either storage ID or field name.</li>
 * </ol>
 *
 * <p>
 * If a class has multiple {@link OnVersionChange &#64;OnVersionChange}-annotated methods, methods with more specific
 * constraint(s) (i.e., non-zero {@link #oldVersion} and/or {@link #newVersion}) will be invoked first.
 *
 * <p><b>Incompatible Schema Changes</b></p>
 *
 * <p>
 * JSimpleDB supports arbitrary Java model schema changes across schema versions, including adding and removing Java types.
 * As a result, it's possible for the previous version of an object to contain reference fields whose Java types no longer exist
 * in the current Java model. Specifically, this can happen in two ways:
 * <ul>
 * <li>A reference field refers to an object type that no longer exists; or</li>
 * <li>An {@link Enum} field refers to an {@link Enum} type that no longer exists, or whose constants have changed</li>
 * </ul>
 *
 * <p>
 * In these cases, the old field's value cannot be represented in {@code oldValues} using the original Java types.
 * Instead, more generic types are used:
 * <ul>
 * <li>For a reference field whose type no longer exists, the referenced object will be an {@link org.jsimpledb.UntypedJObject}.
 *      Note that the fields in the {@link org.jsimpledb.UntypedJObject} may still be accessed by invoking the
 *      {@link org.jsimpledb.JTransaction} field access methods with {@code upgradeVersion} set to false (otherwise,
 *      a {@link org.jsimpledb.core.TypeNotInSchemaVersionException} is thrown).
 * <li>For {@link Enum} fields whose type no longer exists or has modified constants, the old value
 *      will be represented as an {@link org.jsimpledb.core.EnumValue} object.</li>
 * </ul>
 *
 * <p>
 * In addition to Java types disappearing, it's also possible that the type of a reference field is narrower in the current
 * Java code than it was in the previous Java code. If an object held a reference in such a field to another object outside
 * the new, narrower type, then upgrading the object without change would represent a violation of Java type safety.
 * Therefore, when any object is upgraded, all references that would otherwise be illegal are cleared (in the manner of
 * {@link org.jsimpledb.core.DeleteAction#UNREFERENCE}); use {@code oldValues} to access the previous field value(s) if needed.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface OnVersionChange {

    /**
     * Required old schema version.
     *
     * <p>
     * If this property is set to a positive value, only version changes
     * for which the previous schema version equals the specified version will result in notification,
     * and the annotated method must have the corresponding parameter omitted. Otherwise notifications
     * are delivered for any previous schema version and the {@code oldVersion} method parameter is required.
     *
     * <p>
     * Negative values are not allowed.
     *
     * @return old schema version, or zero for no restriction
     */
    int oldVersion() default 0;

    /**
     * Required new schema version.
     *
     * <p>
     * If this property is set to a positive value, only version changes
     * for which the new schema version equals the specified version will result in notification,
     * and the annotated method must have the corresponding parameter omitted. Otherwise notifications
     * are delivered for any new schema version and the {@code newVersion} method parameter is required.
     *
     * <p>
     * Negative values are not allowed.
     *
     * @return new schema version, or zero for no restriction
     */
    int newVersion() default 0;

    /**
     * Configure whether to always use {@link org.jsimpledb.core.EnumValue} objects to represent
     * the values in {@link Enum} fields.
     *
     * <p>
     * When populating the {@code oldValues} map for an {@link Enum} field, if the field's type has changed
     * an {@link org.jsimpledb.core.EnumValue} object must be used to represent the {@link Enum} constant in the
     * original {@link Enum} type, which may no longer exist. When an {@link Enum} field's type has not changed,
     * the actual {@link Enum} constants are used.
     *
     * <p>
     * Setting this property to true will force {@link Enum} field values to always be represented as
     * {@link org.jsimpledb.core.EnumValue} objects, regardless of whether the field's type has changed,
     * making these values consistent in all cases.
     *
     * <p>
     * <b>Note:</b> this property's default will change to true in a future release, and eventually it will
     * be deprecated and eliminated. Existing {@link OnVersionChange &#64;OnVersionChange} methods that expect
     * {@link Enum} instances should be updated to use {@link org.jsimpledb.core.EnumValue} instances instead.
     *
     * @see org.jsimpledb.core.EnumValue#find EnumValue.find()
     * @see Enum#valueOf Enum.valueOf()
     */
    boolean alwaysUseEnumValue() default false;
}

