
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.core.ObjId;

/**
 * Interface implemented by {@link JSimpleDB} Java model objects.
 *
 * <p>
 * All {@link JSimpleDB} database objects are instances of runtime-generated sub-classes of the provided Java model classes.
 * These generated subclasses will always implement this interface, providing convenient access to database operations.
 * Therefore, it is conveninent to declare Java model classes {@code abstract} and {@code implements JObject}.
 * However, this is not strictly necessary; all of the methods declared here ultimately delegate to one of the
 * {@link JTransaction} support methods.
 * </p>
 *
 * <p>
 * There are two types of {@link JObject}s: normal instances, which always reflect the state of the {@link JTransaction}
 * {@linkplain JTransaction#getCurrent associated} with the current thread, and "snapshot" instances that reflect
 * the state of their associated {@link SnapshotJTransaction}. Use {@link #isSnapshot} to distinguish if necessary.
 * For example, some create/delete/change callback methods may not apply in a snapshot context.
 * </p>
 *
 * <p>
 * The {@link #getTransaction} method returns the transaction associated with an instance. For a normal instance,
 * this is just the {@link JTransaction} {@linkplain JTransaction#getCurrent associated} with the current thread;
 * if there is no such transaction, an {@link IllegalStateException} is thrown. For a "snapshot" instance,
 * {@link #getTransaction} always returns the corresponding {@link SnapshotJTransaction}.
 * </p>
 */
public interface JObject {

    /**
     * Get this instance's unique JSimpleDB object identifier.
     *
     * <p>
     * This method always succeeds.
     * </p>
     *
     * @return unique database identifier for this instance
     */
    ObjId getObjId();

    /**
     * Get this instance's current schema version. Does not change this instance's schema version.
     *
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if this is not a snapshot instance and the transaction associated with the current thread is no longer usable
     */
    int getSchemaVersion();

    /**
     * Get this instance's associated {@link JTransaction}.
     *
     * <p>
     * If this is a regular database instance, this returns the {@link JTransaction}
     * {@linkplain JTransaction#getCurrent associated} with the current thread. Otherwise, this instance
     * is a snapshot instance and this method returns the associated {@link SnapshotJTransaction}.
     * </p>
     *
     * @return the {@link JTransaction} that contains this instance's field state
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     */
    JTransaction getTransaction();

    /**
     * Delete this instance, if it exists, in this instance's associated transaction.
     *
     * @return true if instance was deleted, false if it did not exist
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if the transaction associated with the current thread is no longer usable
     * @throws org.jsimpledb.core.ReferencedObjectException if the object is referenced by some other object
     *  through a reference field configured for {@link org.jsimpledb.core.DeleteAction#EXCEPTION}
     */
    boolean delete();

    /**
     * Determine whether this instance still exists in its associated transaction.
     *
     * @return true if instance exists, otherwise false
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if this is not a snapshot instance and the transaction associated with the current thread is no longer usable
     */
    boolean exists();

    /**
     * Determine whether this instance is a normal instance or is a "snapshot" instance associated
     * with a {@link SnapshotJTransaction}.
     *
     * @return true if instance is a snapshot instance
     */
    boolean isSnapshot();

    /**
     * Duplicate this instance. Creates a new instance (with a new {@linkplain #getObjId object ID}) having the
     * same type as this instance and whose fields contain identical content. No schema version upgrade is performed.
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and {@link org.jsimpledb.annotation.OnCreate &#64;OnChange}
     * notifications will be delivered accordingly.
     *
     * <p>
     * This method can be thought of as the {@link JSimpleDB} database equivalent of {@link Object#clone}.
     * </p>
     *
     * @return clone of this instance
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if this is not a snapshot instance and the transaction associated with the current thread is no longer usable
     */
    JObject duplicate();

    /**
     * Recreate a deleted instance, if it does not exist, in its associated transaction.
     * The fields of a recreated object are set to their initial values.
     *
     * @return true if instance was recreated, false if it already existed
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if this is not a snapshot instance and the transaction associated with the current thread is no longer usable
     */
    boolean recreate();

    /**
     * Add this instance to the validation queue for validation in its associated transaction.
     *
     * <p>
     * The actual validation will occur either during {@link JTransaction#commit}
     * or at the next invocation of {@link JTransaction#validate}, whichever occurs first.
     * </p>
     *
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if this is not a snapshot instance and the transaction associated with the current thread is no longer usable
     */
    void revalidate();

    /**
     * Update the schema version of this instance, if necessary, so that it matches the schema version
     * of its associated transaction.
     *
     * <p>
     * If a version change occurs, matching {@link org.jsimpledb.annotation.OnVersionChange &#64;OnVersionChange}
     * methods will be invoked prior to this method returning.
     * </p>
     *
     * @return true if the object's schema version was changed, false if it was already updated
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if this is not a snapshot instance and the transaction associated with the current thread is no longer usable
     */
    boolean upgrade();

    /**
     * Copy this instance, and other instances it references, into another {@link JTransaction}.
     *
     * <p>
     * This method will copy this object and all of its fields, along all other objects reachable through
     * any of the specified {@linkplain ReferencePath reference paths}, into the specified {@link JTransaction}.
     * If any object already exists in {@code dest}, it will be overwritten, otherwise it will be created.
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and {@link org.jsimpledb.annotation.OnCreate &#64;OnChange}
     * notifications will be delivered accordingly.
     * </p>
     *
     * @param dest destination transaction for copies
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the copied version of this instance
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.SchemaMismatchException
     *  if the schema corresponding to this object's version is not identical in both transactions
     * @throws IllegalArgumentException if {@code dest} or {@code refPaths} is null
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyIn copyIn()
     * @see #copyOut copyOut()
     */
    JObject copyTo(JTransaction dest, String... refPaths);

    /**
     * Snapshot this instance and other instances it references.
     *
     * <p>
     * This method will copy this object and all of its fields, along all other objects reachable through
     * any of the specified {@linkplain ReferencePath reference paths}, into the {@link SnapshotJTransaction}
     * {@linkplain JTransaction#getSnapshotTransaction associated} with this instance's associated transaction.
     * If any object already exists there, it will be overwritten, otherwise it will be created.
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and {@link org.jsimpledb.annotation.OnCreate &#64;OnChange}
     * notifications will be delivered accordingly.
     * </p>
     *
     * <p>
     * Normally this method would only be invoked on a regular database {@link JObject}.
     * The returned object will always be a snapshot {@link JObject}.
     * </p>
     *
     * <p>
     * This is a convenience method, equivalent to:
     *  <blockquote><code>
     *  copyTo(JTransaction.getCurrent().getSnapshotTransaction(), refPaths);
     *  </code></blockquote>
     * </p>
     *
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the snapshot {@link JObject} copy of this instance
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws IllegalArgumentException if {@code refPaths} is null
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyIn copyIn()
     */
    JObject copyOut(String... refPaths);

    /**
     * Copy this instance and other instances it references into the transaction associated with the current thread.
     *
     * <p>
     * This method will copy this object and all of its fields, along all other objects reachable through
     * any of the specified {@linkplain ReferencePath reference paths}, into the {@link JTransaction}
     * {@linkplain JTransaction#getCurrent associated} with the current thread.
     * If any object already exists in the current thread's transaction, it will be overwritten, otherwise it will be created.
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and {@link org.jsimpledb.annotation.OnCreate &#64;OnChange}
     * notifications will be delivered accordingly.
     * </p>
     *
     * <p>
     * Normally this method would only be invoked on a snapshot {@link JObject}.
     * The returned object will always be a regular database {@link JObject}.
     * </p>
     *
     * <p>
     * This is a convenience method, equivalent to {@code copyIn(JTransaction.getCurrent(), refPaths)}.
     * </p>
     *
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the regular database copy of this instance
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     * @throws org.jsimpledb.core.SchemaMismatchException
     *  if the schema corresponding to this object's version is not identical in both transactions
     * @throws IllegalArgumentException if {@code refPaths} is null
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyOut copyOut()
     */
    JObject copyIn(String... refPaths);
}

