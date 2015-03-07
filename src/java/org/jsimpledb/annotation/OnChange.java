
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
 * Annotation for methods that are to be invoked whenever a target field in some target object changes during a transaction,
 * where the target object containing the changed field is found at the end of a path of references
 * starting from the object to be notified.
 * See {@link org.jsimpledb.ReferencePath} for more information about reference paths.
 *
 * <p><b>Method Parameter Types</b></p>
 *
 * <p>
 * In all cases the annotated method must return void and take a single parameter, which is compatible with one or more
 * of the {@link org.jsimpledb.change.FieldChange} sub-types appropriate for the field being watched.
 * The method may have any level of access, including {@code private}.
 * The method parameter type can be used to restrict which notifications are delivered. For example, an annotated method
 * taking a {@link org.jsimpledb.change.SetFieldChange} will receive notifications about all changes to a set field,
 * while a method taking a {@link org.jsimpledb.change.SetFieldAdd} will receive notification only when an element
 * is added to the set.
 * </p>
 *
 * <p>
 * Multiple reference paths may be specified; if so, all of the specified paths are monitored together, and they all
 * must emit {@link org.jsimpledb.change.FieldChange}s compatible with the method's parameter type. Therefore, when
 * mutiple fields are monitored, the method's parameter type may need to be widened (either in raw type, generic type
 * parameters, or both).
 * </p>
 *
 * <p>
 * As a special case, if zero fields are specified, then "wildcard" monitoring of every field in the local object occurs.
 * However in this case, only fields that emit changes compatible with the method's parameter type will be monitored.
 * So for example, a method taking a {@link org.jsimpledb.change.SetFieldChange} would receive notifications about
 * changes to all {@code Set} fields in the class, but not any other fields.
 * </p>
 *
 * <p><b>Instance vs. Static Methods</b></p>
 *
 * <p>
 * If the method is an instance method, then {@link #startType} must be left unset; if the instance is a static
 * method, then {@link #startType} may be explicitly set, or if left unset it defaults to the class containing
 * the annotated method.
 * </p>
 *
 * <p>
 * For an instance method, the method will be invoked on <i>each object</i> for which the changed field is
 * found at the end of the specified reference path <i>starting from that object</i>.
 * </p>
 *
 * <p>
 * If the annotated method is a static method, the method is invoked <i>once</i> if any instance exists for which the
 * changed field is found at the end of the specified reference path, no matter how many such instances there are.
 * Otherwise the behavior is the same.
 * </p>
 *
 * <p><b>Notification Delivery</b></p>
 *
 * <p>
 * Notifications are delivered synchronously within the thread the made the change, after the change is made and just
 * prior to returning to the original caller.
 * Additional changes made within an {@link OnChange &#64;OnChange} handler that themselves result in notifications
 * are also handled prior to returning to the original caller. Therefore, infinite loops are possible if an
 * {@link OnChange &#64;OnChange} handler method modifies the field it's monitoring (directly, or indirectly via
 * other {@link OnChange &#64;OnChange} handler methods).
 * </p>
 *
 * <p>
 * {@link OnChange &#64;OnChange} functions within a single transaction; it does not notify about changes that
 * may have occurred in a different transaction.
 * </p>
 *
 * <p><b>Other Notes</b></p>
 *
 * <p>
 * No notifications are delivered for "changes" that do not actually change anything (e.g., setting a simple field to
 * the value already contained in that field, or adding an element to a set which is already contained in the set).
 * </p>
 *
 * <p>
 * For any given field change and path, only one notification will be delivered per recipient object, even if the changed field
 * is seen through the path in multiple ways (e.g., via reference path {@code "mylist.element.myfield"} where the changed object
 * containing {@code myfield} appears multiple times in {@code mylist}).
 * </p>
 *
 * <p>
 * See {@link org.jsimpledb.core.Transaction#addSimpleFieldChangeListener Transaction.addSimpleFieldChangeListener()}
 * for further information on other special corner cases.
 * </p>
 *
 * @see org.jsimpledb.ReferencePath
 * @see org.jsimpledb.change
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface OnChange {

    /**
     * Specifies the path to the target field to watch for changes.
     * See {@link org.jsimpledb.ReferencePath} for information on the proper syntax.
     *
     * <p>
     * Multiple paths may be specified; if so, each path is handled as a separate independent listener registration,
     * and the method's parameter type must be compatible with at least one of the {@link org.jsimpledb.change.FieldChange}
     * sub-types emitted by each field. If zero paths are specified, every field in the class (including superclasses)
     * that emits {@link org.jsimpledb.change.FieldChange}s compatible with the method parameter will be monitored for changes.
     * </p>
     *
     * @return reference path leading to the changed field
     * @see org.jsimpledb.ReferencePath
     */
    String[] value() default { };

    /**
     * Specifies the starting type for the {@link org.jsimpledb.ReferencePath} specified by {@link #value}.
     *
     * <p>
     * This property must be left unset for instance methods. For static methods, if this property is left unset,
     * then then class containing the annotated method is assumed.
     * </p>
     *
     * @return Java type at which the reference path starts
     * @see org.jsimpledb.ReferencePath
     */
    Class<?> startType() default void.class;

    /**
     * Determines whether this annotation should also be enabled for
     * {@linkplain org.jsimpledb.SnapshotJTransaction snapshot transaction} objects.
     * If unset, notifications will only be delivered to non-snapshot (i.e., normal) database instances.
     *
     * @return whether enabled for snapshot transactions
     * @see org.jsimpledb.SnapshotJTransaction
     */
    boolean snapshotTransactions() default false;
}

