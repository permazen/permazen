
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.core.ObjId;

/**
 * Interface implemented by {@link JSimpleDB} Java model objects.
 *
 * <p>
 * All {@link JSimpleDB} database objects are instances of runtime-generated sub-classes of user-provided Java model classes.
 * These generated subclasses will always implement this interface, providing convenient access to database operations.
 * Therefore, it is conveninent to declare Java model classes {@code abstract} and {@code implements JObject}.
 * However, this is not strictly necessary; all of the methods declared here ultimately delegate to one of the
 * {@link JTransaction} support methods.
 *
 * <p>
 * All {@link JObject}s are {@linkplain #getTransaction associated} with a specific {@linkplain JTransaction},
 * and are the unique representatives for their corresponding {@link ObjId} in that transaction.
 * All field state derives from the transaction.
 *
 * @see JTransaction
 */
public interface JObject {

    /**
     * Get this instance's unique JSimpleDB object identifier.
     *
     * <p>
     * This method always succeeds, even if the object does not {@linkplain #exists exist}.
     *
     * @return unique database identifier for this instance
     */
    ObjId getObjId();

    /**
     * Get this instance's current schema version. Does not change this instance's schema version.
     *
     * @return the schema version of this instance
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    int getSchemaVersion();

    /**
     * Get this instance's associated {@link JTransaction}.
     *
     * <p>
     * If this is a regular database instance, this returns the {@link JTransaction}
     * {@linkplain JTransaction#getCurrent associated} with the current thread. Otherwise, this instance
     * is a snapshot instance and this method returns the associated {@link SnapshotJTransaction}.
     *
     * @return the {@link JTransaction} that contains this instance's field state
     */
    JTransaction getTransaction();

    /**
     * Delete this instance, if it exists, in this instance's associated transaction.
     *
     * <p>
     * See {@link org.jsimpledb.core.Transaction#delete Transaction.delete()} for details on secondary deletions from
     * {@link org.jsimpledb.core.DeleteAction#DELETE} and {@link org.jsimpledb.annotation.JField#cascadeDelete}.
     *
     * @return true if instance was deleted, false if it did not exist
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
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
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
     * Recreate a deleted instance, if it does not exist, in its associated transaction.
     * The fields of a recreated object are set to their initial values. If the object already exists, nothing changes.
     *
     * @return true if instance was recreated, false if it already existed
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    boolean recreate();

    /**
     * Add this instance to the validation queue for validation in its associated transaction.
     *
     * <p>
     * The actual validation will occur either during {@link JTransaction#commit}
     * or at the next invocation of {@link JTransaction#validate}, whichever occurs first.
     * The specified validation groups, if any, will be used.
     *
     * <p>
     * If the associated transaction was opened with {@link ValidationMode#DISABLED}, no validation will be performed.
     *
     * @param groups validation group(s) to use for validation; if empty, {@link javax.validation.groups.Default} is assumed
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     * @throws NullPointerException if {@code groups} is null
     */
    void revalidate(Class<?>... groups);

    /**
     * Update the schema version of this instance, if necessary, so that it matches the schema version
     * of its associated transaction.
     *
     * <p>
     * If a version change occurs, matching {@link org.jsimpledb.annotation.OnVersionChange &#64;OnVersionChange}
     * methods will be invoked prior to this method returning.
     *
     * @return true if the object's schema version was changed, false if it was already updated
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    boolean upgrade();

    /**
     * Copy this instance, and other instances it references through the specified {@code refPaths} (if any), into a (possibly)
     * different {@link JTransaction}. This is a more general method; see {@link #copyIn copyIn()} and {@link #copyOut copyOut()}
     * for more common and specific use cases.
     *
     * <p>
     * The "granularity" of the copy operation is one object, which includes all of the object's fields. This method will
     * always copy at least this instance, and possibly others if {@code refPaths} is non-empty.
     *
     * <p>
     * This method will copy this object's fields into the object with ID {@code target} (or this instance's object ID if
     * {@code target} is null) in the {@code dest} transaction, overwriting any previous values there, along with all other
     * objects reachable from this instance through any of the specified {@linkplain ReferencePath reference paths} (including
     * intermediate objects visited). This instance will first be {@link #upgrade}ed if necessary.
     * If {@code target} (or any other referenced object) already exists in {@code dest}, it will have its schema version
     * updated first, if necessary, otherwise it will be created.
     * Any {@link org.jsimpledb.annotation.OnCreate &#64;OnVersionChange}, {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate},
     * and {@link org.jsimpledb.annotation.OnCreate &#64;OnChange} methods will be notified accordingly as usual (in {@code dest});
     * however, for {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnChange}, the annotation must have {@code snapshotTransactions = true}
     * if {@code dest} is a {@link SnapshotJTransaction}.
     *
     * <p>
     * The two transactions must be compatible in that for any schema versions encountered, those schema versions
     * must be identical in both transactions.
     *
     * <p>
     * Circular references are handled properly: if an object is encountered more than once, it is not copied again.
     * The {@code copyState} parameter can be used to keep track of objects that have already been copied and/or traversed
     * along some reference path (however, if an object is marked as copied in {@code copyState} and is traversed, but does not
     * actually already exist in {@code dest}, an exception is thrown).
     * For a "fresh" copy operation, pass a newly created {@code CopyState}; for a copy operation that is a continuation
     * of a previous copy, reuse the previous {@link CopyState}.
     *
     * <p>
     * Warning: if two threads attempt to copy objects between the same two transactions at the same time
     * but in opposite directions, deadlock could result.
     *
     * @param dest destination transaction for copies
     * @param target target object ID in {@code dest} onto which to copy this instance's fields, or null for this instance
     * @param copyState tracks which indirectly referenced objects have already been copied
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the copied version of this instance in {@code dest}
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     *  (no exception is thrown however if an indirectly referenced object does not exist unless it is traversed)
     * @throws org.jsimpledb.core.DeletedObjectException if an object in {@code copied} is traversed but does not actually exist
     * @throws org.jsimpledb.core.SchemaMismatchException
     *  if the schema corresponding to this object's version is not identical in both the {@link JTransaction}
     *  associated with this instance and {@code dest} (as well for any referenced objects)
     * @throws IllegalArgumentException if {@code dest}, {@code copied}, or {@code refPaths} is null
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyIn copyIn()
     * @see #copyOut copyOut()
     * @see JTransaction#copyTo(JTransaction, JObject, ObjId, CopyState, String[]) JTransaction.copyTo()
     */
    JObject copyTo(JTransaction dest, ObjId target, CopyState copyState, String... refPaths);

    /**
     * Snapshot this instance and other instances it references through the specified {@code refPaths} (if any).
     *
     * <p>
     * The "granularity" of the copy operation is one object, which includes all of the object's fields. This method will
     * always copy at least this instance, and possibly others if {@code refPaths} is non-empty.
     *
     * <p>
     * This method will copy this object and all of its fields, along with all other objects reachable through
     * any of the specified {@linkplain ReferencePath reference paths} into the {@link SnapshotJTransaction}
     * {@linkplain JTransaction#getSnapshotTransaction corresponding} to this instance's associated transaction
     * (including intermediate objects visited). This instance will first be {@link #upgrade}ed if necessary.
     * If any object already exists there, it will be overwritten, otherwise it will be created.
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and {@link org.jsimpledb.annotation.OnCreate &#64;OnChange}
     * notifications will be delivered accordingly; however, the annotation must have {@code snapshotTransactions = true}.
     *
     * <p>
     * Normally this method would only be invoked on a regular database {@link JObject}.
     * The returned object will always be a snapshot {@link JObject}.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><code>
     * this.copyTo(this.getTransaction().getSnapshotTransaction(), null, new CopyState(), refPaths);
     * </code></blockquote>
     *
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the snapshot {@link JObject} copy of this instance
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     *  (no exception is thrown however if an indirectly referenced object does not exist)
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyIn copyIn()
     */
    JObject copyOut(String... refPaths);

    /**
     * Copy this instance, and other instances it references through the specified {@code refPaths} (if any),
     * into the transaction associated with the current thread.
     *
     * <p>
     * The "granularity" of the copy operation is one object, which includes all of the object's fields. This method will
     * always copy at least this instance, and possibly others if {@code refPaths} is non-empty.
     *
     * <p>
     * This method will copy this object and all of its fields, along with all other objects reachable through any of the
     * specified {@linkplain ReferencePath reference paths} into the {@link JTransaction}
     * {@linkplain JTransaction#getCurrent associated} with the current thread (including intermediate objects visited).
     * If any object already exists in the current thread's transaction, it will be overwritten, otherwise it will be created.
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and {@link org.jsimpledb.annotation.OnCreate &#64;OnChange}
     * notifications will be delivered accordingly.
     *
     * <p>
     * Normally this method would only be invoked on a snapshot {@link JObject}.
     * If this instance is a regular database {@link JObject}, then it is immediately returned unchanged.
     * The returned object will always be a regular database {@link JObject}.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><code>
     * this.copyTo(JTransaction.getCurrent(), null, new CopyState(), refPaths)
     * </code></blockquote>
     *
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the regular database copy of this instance
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     *  (no exception is thrown however if an indirectly referenced object does not exist)
     * @throws org.jsimpledb.core.SchemaMismatchException
     *  if the schema corresponding to this object's version is not identical in both transactions
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyOut copyOut()
     */
    JObject copyIn(String... refPaths);

    /**
     * Reset cached simple field values.
     *
     * <p>
     * {@link JObject}s instances may cache simple field values after they have been read from the underlying
     * key/value store for efficiency. This method causes any such cached values to be forgotten, so they will
     * be re-read from the underlying key/value store on the next read of the field.
     *
     * <p>
     * Normally this method does not need to be used. It may be needed to maintain consistency
     * in exotic situations, for example, where the underlying key/value store is being modified directly.
     */
    void resetCachedFieldValues();
}

