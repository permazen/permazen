
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

/**
 * Annotation for methods that are to be invoked whenever an object's schema version is changed.
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and
 * take one, two, or three of the following parameters in this order:
 *  <ol>
 *  <li>{@code int oldVersion} - previous schema version; should be present only if {@link #oldVersion} is zero</li>
 *  <li>{@code int newVersion} - new schema version (always equal to
 *      {@link org.jsimpledb.core.Transaction}.{@link org.jsimpledb.core.Transaction#getSchemaVersion getSchemaVersion()});
 *      should be present only if {@link #newVersion} is zero</li>
 *  <li>{@code Map<Integer, Object> oldFieldValues} - contains all field values from the previous version of the object,
 *      indexed by storage ID.</li>
 *  </ol>
 * </p>
 *
 * <p>
 * If a class has multiple {@link OnVersionChange &#64;OnVersionChange}-annotated methods, methods with a non-zero
 * {@link #oldVersion} or {@link #newVersion} (i.e., more specific constraint) will be invoked before methods having
 * no constraint when possible.
 * </p>
 *
 * <p>
 * Note that the old schema version may contain fields whose Java types no longer exist in the current Java code base.
 * Specifically, this can (only) occur in these two cases:
 *  <ul>
 *  <li>A reference field that refers to an object type that no longer exists; and</li>
 *  <li>An {@link Enum} field whose {@link Enum} type no longer exists or whose values have changed</li>
 *  </ul>
 * In these cases, the old field's value cannot be represented using the original Java types in the {@code oldFieldValues} map.
 * Instead, less specific types are substituted:
 *  <ul>
 *  <li>For reference fields, in the case no Java type in the current schema corresponds to the referred-to object type,
 *      the object will be represented as an {@link org.jsimpledb.UntypedJObject}. Note the fields of the
 *      {@link org.jsimpledb.UntypedJObject} may still be accessed via {@link org.jsimpledb.JTransaction} field access methods.</li>
 *  <li>For {@link Enum} fields, if the old {@link Enum} type is not found, or any of its values have changed name or ordinal,
 *      then the old field's value will be represented as an {@link org.jsimpledb.core.EnumValue}.</li>
 *  </ul>
 * </p>
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
     * </p>
     *
     * <p>
     * Negative values are not allowed.
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
     * </p>
     *
     * <p>
     * Negative values are not allowed.
     */
    int newVersion() default 0;
}

