
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
 * Annotates JSimpleDB model class methods that are to be invoked whenever a database object is newly created.
 *
 * <p>
 * Note that there is a subtle distinction between (a) the creation of database objects in the database, and
 * (b) the instantiation of Java model objects that represent database objects (i.e., {@link org.jsimpledb.JObject}s).
 * These two events do not necessarily always occur at the same time. Methods that are annotated with
 * {@link OnCreate &#64;OnCreate} are invoked only for event (a). In particular, Java model objects are often
 * instantiated to represent database objects that may not actually exist in the database.
 * </p>
 *
 * <p>
 * As a consequence, for any database fields that require default initialization, this initialization should be
 * performed not in a Java constructor but rather in an {@link OnCreate &#64;OnCreate}-annotated method. Otherwise,
 * {@link org.jsimpledb.core.DeletedObjectException}s (and {@link org.jsimpledb.core.ReadOnlyTransactionException}s
 * within read-only transactions) may unexpectedly occur.
 * </p>
 *
 * <p>
 * For example, instead of this:
 * <pre>
 *  &#64;JSimpleClass
 *  public abstract class Event {
 *
 *      protected Event() {
 *          this.setCreateTime(new Date());
 *      }
 *
 *      public abstract Date getCreateTime();
 *      public abstract void setCreateTime(Date createTime);
 *
 *      ...
 * </pre>
 * do this:
 *
 * <pre>
 *  &#64;JSimpleClass
 *  public abstract class Event {
 *
 *      protected Event() {
 *      }
 *
 *      &#64;OnCreate
 *      private void init() {
 *          this.setCreateTime(new Date());
 *      }
 *
 *      public abstract Date getCreateTime();
 *      public abstract void setCreateTime(Date createTime);
 *
 *      ...
 * </pre>
 *
 * <p>
 * Notifications are delivered in the same thread that created the object, immediately after the object is created.
 * </p>
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and take zero parameters.
 * It may have any level of access, including {@code private}.
* </p>
*/
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface OnCreate {

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

