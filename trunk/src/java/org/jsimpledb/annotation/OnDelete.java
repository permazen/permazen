
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
 * Annotates JSimpleDB model class methods that are to be invoked whenever an object is about to be deleted.
 *
 * <p>
 * Notifications are delivered in the same thread that deletes the object, before the delete actually occurs.
 * At most one delete notification will ever be delivered for any object deletion event.
 * </p>
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and take zero parameters.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface OnDelete {

    /**
     * Determines whether this annotation should be enabled for
     * {@linkplain org.jsimpledb.SnapshotJTransaction snapshot transactions}.
     *
     * @see org.jsimpledb.SnapshotJTransaction
     */
    boolean snapshotTransactions() default false;
}

