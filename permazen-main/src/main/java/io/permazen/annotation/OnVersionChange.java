
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for methods that are to be invoked whenever an object's schema version has just changed.
 *
 * <p>
 * The annotated method is given access to all of the previous object version's fields, including fields that have
 * been deleted or whose types have changed in the new schema version. This allows the object to perform any
 * schema migration "fixups" required before the old information is lost.
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
 * <li>For a reference field whose type no longer exists, the referenced object will be an {@link io.permazen.UntypedJObject}.
 *      Note that the fields in the {@link io.permazen.UntypedJObject} may still be accessed by invoking the
 *      {@link io.permazen.JTransaction} field access methods with {@code upgradeVersion} set to false (otherwise,
 *      a {@link io.permazen.core.TypeNotInSchemaVersionException} is thrown).
 * <li>For {@link Enum} fields, the old value will always be represented as an {@link io.permazen.core.EnumValue} object.</li>
 * </ul>
 *
 * <p>
 * In addition to Java types disappearing, it's also possible that the type of a reference field is narrower in the current
 * Java code than it was in the previous Java code. If an object held a reference in such a field to another object outside
 * the new, narrower type, then upgrading the object without change would represent a violation of Java type safety.
 * Therefore, when any object is upgraded, all references that would otherwise be illegal are cleared (in the manner of
 * {@link io.permazen.core.DeleteAction#UNREFERENCE}); use {@code oldValues} to access the previous field value(s) if needed.
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
}

