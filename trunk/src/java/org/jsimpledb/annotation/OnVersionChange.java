
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
 *      {@link org.jsimpledb.Transaction}.{@link org.jsimpledb.Transaction#getSchemaVersion getSchemaVersion()});
 *      should be present only if {@link #newVersion} is zero</li>
 *  <li>{@code Map<Integer, Object> oldFieldValues} - contains all field values from the previous version of the object
 *      indexed by storage ID</li>
 *  </ol>
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

