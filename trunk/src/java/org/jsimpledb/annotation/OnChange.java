
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
 * Annotation for methods that are to be invoked whenever a targe field in some target object changes,
 * where the target object containing the changed field is found at the end of a path of references
 * starting from the object to be notified.
 * See {@link org.jsimpledb.jlayer.ReferencePath} for more information about reference paths.
 *
 * <p><b>Method Parameter Types</b></p>
 *
 * <p>
 * In all cases the annotated method must return void and take a single parameter, which is a
 * sub-type of {@link org.jsimpledb.jlayer.change.FieldChange} suitable for the field being watched.
 * The parameter type can be used to restrict which notifications are delivered. For example, an annotated method
 * taking a {@link org.jsimpledb.jlayer.change.SetFieldChange} will receive notifications about all changes to a set field,
 * while a method taking a {@link org.jsimpledb.jlayer.change.SetFieldAdd} will receive notification only when an element
 * is added to the set.
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
 * found at the end of the specified reference path <i>starting from that object</i>. Notifications are delivered from
 * within the thread the made the change, after the change is made and just prior to returning to the original caller.
 * Additional changes made within an {@link OnChange &#64;OnChange} handler that themselves result in new notifications
 * are also handled prior to returning to the original caller. Therefore, infinite loops are possible if an
 * {@link OnChange &#64;OnChange} handler method modifies the field it's monitoring (directly, or indirectly via
 * other {@link OnChange &#64;OnChange} handler methods).
 * </p>
 *
 * <p>
 * If the annotated method is a static method, the method is invoked <i>once</i> if any instance exists for which the
 * changed field is found at the end of the specified reference path (no matter how many such instances there are).
 * Otherwise the behavior is the same.
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
 * For any given field change, only one notification will be delivered per recipient object, even if the changed field is seen
 * through the path in multiple ways (e.g., via reference path {@code "mylist.element.myfield"} where the changed object
 * containing {@code myfield} appears multiple times in {@code mylist}).
 * </p>
 *
 * <p>
 * See {@link org.jsimpledb.Transaction#addSimpleFieldChangeListener Transaction.addSimpleFieldChangeListener()}
 * for further information on other special corner cases.
 * </p>
 *
 * @see org.jsimpledb.jlayer.ReferencePath
 * @see org.jsimpledb.jlayer.change
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface OnChange {

    /**
     * Specifies the path to the target field to watch for changes.
     * See {@link org.jsimpledb.jlayer.ReferencePath} for information on the proper syntax.
     *
     * @see org.jsimpledb.jlayer.ReferencePath
     */
    String value();

    /**
     * Specifies the starting type for the {@link org.jsimpledb.jlayer.ReferencePath} specified by {@code #value}.
     *
     * <p>
     * This property must be left unset for instance methods. For static methods, if this property is left unset,
     * then then class containing the annotated method is assumed.
     * </p>
     *
     * @see org.jsimpledb.jlayer.ReferencePath
     */
    Class<?> startType() default void.class;
}

