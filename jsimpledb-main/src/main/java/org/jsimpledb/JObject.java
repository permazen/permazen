
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
    default int getSchemaVersion() {
        return this.getTransaction().getSchemaVersion(this.getObjId());
    }

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
    default boolean delete() {
        return this.getTransaction().delete(this);
    }

    /**
     * Determine whether this instance still exists in its associated transaction.
     *
     * @return true if instance exists, otherwise false
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    default boolean exists() {
        return this.getTransaction().exists(this.getObjId());
    }

    /**
     * Determine whether this instance is a normal instance or is a "snapshot" instance associated
     * with a {@link SnapshotJTransaction}.
     *
     * <p>
     * Equvialent to {@code getTransaction().isSnapshot()}.
     *
     * @return true if instance is a snapshot instance
     */
    default boolean isSnapshot() {
        return this.getTransaction().isSnapshot();
    }

    /**
     * Recreate a deleted instance, if it does not exist, in its associated transaction.
     * The fields of a recreated object are set to their initial values. If the object already exists, nothing changes.
     *
     * @return true if instance was recreated, false if it already existed
     * @throws org.jsimpledb.core.StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    default boolean recreate() {
        return this.getTransaction().recreate(this);
    }

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
    default void revalidate(Class<?>... groups) {
        this.getTransaction().revalidate(this.getObjId(), groups);
    }

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
    default boolean upgrade() {
        return this.getTransaction().updateSchemaVersion(this);
    }

    /**
     * Copy this instance, and other instances it references through the specified reference paths, into the
     * specified destination transaction.
     *
     * <p>
     * This is a more general method; see {@link #copyIn copyIn()} and {@link #copyOut copyOut()}
     * for more common and specific use cases.
     *
     * <p>
     * This method copies this object and all other objects reachable from this instance through any of the specified
     * {@linkplain ReferencePath reference paths} (including intermediate objects visited).
     *
     * <p>
     * This instance will first be {@link #upgrade}ed if necessary. If any copied object already exists in {@code dest},
     * it will have its schema version updated first, if necessary, then be overwritten.
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
     * The {@code copyState} tracks which objects have already been copied and/or traversed along some reference path.
     * For a "fresh" copy operation, pass a newly created {@code CopyState}; for a copy operation that is a continuation
     * of a previous copy, reuse the previous {@code copyState}. The {@code CopyState} may also be configured to remap object ID's.
     *
     * <p>
     * Warning: if two threads attempt to copy objects between the same two transactions at the same time
     * but in opposite directions, deadlock could result.
     *
     * @param dest destination transaction for copies
     * @param copyState tracks which indirectly referenced objects have already been copied
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the copied version of this instance in {@code dest}
     * @throws org.jsimpledb.core.DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     *  (no exception is thrown however if an indirectly referenced object does not exist unless it is traversed)
     * @throws org.jsimpledb.core.DeletedObjectException if any object to be copied does not actually exist
     * @throws org.jsimpledb.core.DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws org.jsimpledb.core.SchemaMismatchException
     *  if the schema corresponding to any copied object is not identical in both the {@link JTransaction}
     *  associated with this instance and {@code dest}
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyIn copyIn()
     * @see #copyOut copyOut()
     * @see JTransaction#copyTo(JTransaction, JObject, ObjId, CopyState, String[]) JTransaction.copyTo()
     * @see ReferencePath
     */
    default JObject copyTo(JTransaction dest, CopyState copyState, String... refPaths) {
        return this.getTransaction().copyTo(dest, this, copyState, refPaths);
    }

    /**
     * Copy this instance and other instances it references through the specified reference paths (if any)
     * into the associated in-memory snapshot transaction.
     *
     * <p>
     * Normally this method would only be invoked on a regular database {@link JObject}.
     * The returned object will always be a snapshot {@link JObject}.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><code>
     * this.copyTo(this.getTransaction().getSnapshotTransaction(), new CopyState(), refPaths);
     * </code></blockquote>
     *
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the snapshot {@link JObject} copy of this instance
     * @throws org.jsimpledb.core.DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field
     *  {@linkplain org.jsimpledb.annotation.JField#allowDeletedSnapshot configured} to disallow deleted assignment
     *  in snapshot transactions
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyIn copyIn()
     */
    default JObject copyOut(String... refPaths) {
        return this.copyTo(this.getTransaction().getSnapshotTransaction(), new CopyState(), refPaths);
    }

    /**
     * Copy this instance, and other instances it references through the specified {@code refPaths} (if any),
     * into the transaction associated with the current thread.
     *
     * <p>
     * Normally this method would only be invoked on a snapshot {@link JObject}.
     * If this instance is a regular database {@link JObject}, then it is immediately returned unchanged.
     * The returned object will always be a regular database {@link JObject}.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><code>
     * this.copyTo(JTransaction.getCurrent(), new CopyState(), refPaths)
     * </code></blockquote>
     *
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the regular database copy of this instance
     * @throws org.jsimpledb.core.DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field
     *  {@linkplain org.jsimpledb.annotation.JField#allowDeletedSnapshot configured} to disallow deleted assignment
     * @throws org.jsimpledb.core.SchemaMismatchException
     *  if the schema corresponding to this object's version is not identical in both transactions
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyOut copyOut()
     */
    default JObject copyIn(String... refPaths) {
        return this.copyTo(JTransaction.getCurrent(), new CopyState(), refPaths);
    }

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

