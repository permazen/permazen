
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.JField;
import io.permazen.annotation.OnChange;
import io.permazen.annotation.OnCreate;
import io.permazen.annotation.OnVersionChange;
import io.permazen.core.DeleteAction;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ObjId;
import io.permazen.core.ReferencedObjectException;
import io.permazen.core.SchemaMismatchException;
import io.permazen.core.StaleTransactionException;
import io.permazen.core.Transaction;
import io.permazen.core.TypeNotInSchemaVersionException;
import io.permazen.core.util.ObjIdSet;
import io.permazen.util.NavigableSets;

import jakarta.validation.groups.Default;

import java.util.NavigableSet;

/**
 * Interface implemented by {@link Permazen} Java model objects.
 *
 * <p>
 * All {@link Permazen} database objects are instances of runtime-generated sub-classes of user-provided Java model types.
 * These generated subclasses will always implement this interface, providing convenient access to database operations.
 * Therefore, it is convenient to declare Java model classes {@code abstract} and {@code implements JObject}.
 * However, this is not strictly necessary; all of the methods declared here ultimately delegate to one of the
 * {@link JTransaction} support methods.
 *
 * <p><b>Object Identity and State</b></p>
 *
 * <p>
 * Every {@link JObject} has a unique 64-bit object identifier, represented as an {@link ObjId}.
 * All {@link JObject} instances are permanently {@linkplain #getTransaction associated} with a specific
 * {@linkplain JTransaction transaction}, and are the unique representatives for their corresponding {@link ObjId}
 * in that transaction. All field state derives from the associated transaction.
 *
 * <p>
 * There are two types of transactions: normal transactions reflecting an open transaction on the underlying
 * key/value database, and {@linkplain DetachedJTransaction detached transactions}, which are in-memory containers
 * of object data. Detached transactions are fully functional, supporting index queries, object versioning, etc.
 *
 * <p><b>Copying Objects</b></p>
 *
 * <p>
 * This interface provides methods for copying a graph of objects between transactions, for example, from an open
 * key/value database transaction into an in-memory detached transaction ("copy out"), or vice-versa ("copy in").
 * A graph of objects can be copied by specifying a starting object and either a list of reference paths, or
 * a cascade name (see {@link JField &#64;JField}). Object ID's can be remapped during
 * the copy if necessary, e.g., to ensure existing objects are not overwritten (see {@link CopyState}).
 *
 * @see JTransaction
 */
public interface JObject {

    /**
     * Get this instance's unique Permazen object identifier.
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
     * @throws DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    default int getSchemaVersion() {
        return this.getTransaction().getSchemaVersion(this.getObjId());
    }

    /**
     * Get this instance's associated {@link JTransaction}.
     *
     * @return the {@link JTransaction} that contains this instance's field state
     */
    JTransaction getTransaction();

    /**
     * Delete this instance, if it exists, in this instance's associated transaction.
     *
     * <p>
     * See {@link Transaction#delete Transaction.delete()} for details on secondary deletions from
     * {@link DeleteAction#DELETE} and {@link JField#forwardDelete}.
     *
     * @return true if instance was deleted, false if it did not exist
     * @throws StaleTransactionException
     *  if the transaction associated with the current thread is no longer usable
     * @throws ReferencedObjectException if the object is referenced by some other object
     *  through a reference field configured for {@link DeleteAction#EXCEPTION}
     */
    default boolean delete() {
        return this.getTransaction().delete(this);
    }

    /**
     * Determine whether this instance still exists in its associated transaction.
     *
     * @return true if instance exists, otherwise false
     * @throws StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    default boolean exists() {
        return this.getTransaction().exists(this.getObjId());
    }

    /**
     * Determine whether this instance is a normal instance or is a detached instance associated
     * with a {@link DetachedJTransaction}.
     *
     * <p>
     * Equivalent to {@code getTransaction().isDetached()}.
     *
     * @return true if instance lives in a detached transaction
     */
    default boolean isDetached() {
        return this.getTransaction().isDetached();
    }

    /**
     * Recreate a deleted instance, if it does not exist, in its associated transaction.
     * The fields of a recreated object are set to their initial values. If the object already exists, nothing changes.
     *
     * @return true if instance was recreated, false if it already existed
     * @throws StaleTransactionException
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
     * @param groups validation group(s) to use for validation; if empty, {@link Default} is assumed
     * @throws DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws StaleTransactionException
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
     * If a version change occurs, matching {@link OnVersionChange &#64;OnVersionChange}
     * methods will be invoked prior to this method returning.
     *
     * @return true if the object's schema version was changed, false if it was already updated
     * @throws DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     * @throws StaleTransactionException
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
     * Any {@link OnVersionChange &#64;OnVersionChange}, {@link OnCreate &#64;OnCreate},
     * and {@link OnChange &#64;OnChange} methods will be notified accordingly as usual (in {@code dest}).
     *
     * <p>
     * The two transactions must be compatible in that for any schema versions encountered, those schema versions
     * must be identical in both transactions.
     *
     * <p>
     * Circular references are handled properly: if an object is encountered more than once, it is not copied again.
     * The {@code copyState} tracks which objects have already been copied and/or traversed along some reference path.
     * For a "fresh" copy operation, pass a newly created {@link CopyState}; for a copy operation that is a continuation
     * of a previous copy, reuse the previous {@code copyState}. The {@link CopyState} may also be configured to remap object ID's.
     *
     * <p>
     * Warning: if two threads attempt to copy objects between the same two transactions at the same time
     * but in opposite directions, deadlock could result.
     *
     * @param dest destination transaction for copies
     * @param copyState tracks which indirectly referenced objects have already been copied
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the copied version of this instance in {@code dest}
     * @throws DeletedObjectException
     *  if this object does not exist in the {@link JTransaction} associated with this instance
     *  (no exception is thrown however if an indirectly referenced object does not exist unless it is traversed)
     * @throws DeletedObjectException if any object to be copied does not actually exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws SchemaMismatchException
     *  if the schema corresponding to any copied object is not identical in both the {@link JTransaction}
     *  associated with this instance and {@code dest}
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyIn copyIn()
     * @see #copyOut copyOut()
     * @see JTransaction#copyTo(JTransaction, JObject, CopyState, String[]) JTransaction.copyTo()
     * @see ReferencePath
     */
    default JObject copyTo(JTransaction dest, CopyState copyState, String... refPaths) {
        return this.getTransaction().copyTo(dest, this, copyState, refPaths);
    }

    /**
     * Copy this instance and other instances it references through the specified reference paths (if any)
     * into the associated in-memory detached transaction.
     *
     * <p>
     * This method should only be invoked on regular database {@link JObject}s.
     * The returned object will always live in a {@link DetachedJTransaction}.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><pre>
     * this.copyTo(this.getTransaction().getDetachedTransaction(), new CopyState(), refPaths);
     * </pre></blockquote>
     *
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the detached {@link JObject} copy of this instance
     * @throws IllegalArgumentException if this instance is itself a {@linkplain #isDetached detached instance}
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyIn copyIn()
     */
    default JObject copyOut(String... refPaths) {
        return this.copyTo(this.getTransaction().getDetachedTransaction(), new CopyState(), refPaths);
    }

    /**
     * Copy this instance, and other instances it references through the specified {@code refPaths} (if any),
     * into the transaction associated with the current thread.
     *
     * <p>
     * Normally this method would only be invoked on detached database {@link JObject}s.
     * The returned object will be a {@link JObject} in the currently open transaction.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><pre>
     * this.copyTo(JTransaction.getCurrent(), new CopyState(), refPaths)
     * </pre></blockquote>
     *
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the regular database copy of this instance
     * @throws DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field
     *  {@linkplain JField#allowDeleted configured} to disallow deleted assignment
     * @throws SchemaMismatchException
     *  if the schema corresponding to this object's version is not identical in both transactions
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @see #copyOut copyOut()
     */
    default JObject copyIn(String... refPaths) {
        return this.copyTo(JTransaction.getCurrent(), new CopyState(), refPaths);
    }

    /**
     * Copy this instance and all objects reachable from it via the specified cascade into the specified destination transaction.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><pre>
     * this.cascadeCopyTo(dest, cascadeName, -1, clone);
     * </pre></blockquote>
     *
     * @param dest destination transaction for copies
     * @param cascadeName cascade name, or null for no cascade (i.e., copy only this instance)
     * @param clone true to clone objects, i.e., assign the copies new, unused object ID's in {@code dest},
     *  or false to preserve the same object ID's, overwriting any existing objects in {@code dest}
     * @return the copied version of this instance in {@code dest}
     * @throws DeletedObjectException if any object to be copied does not exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws SchemaMismatchException
     *  if the schema version corresponding to any copied object is not identical in both transactions
     * @throws IllegalArgumentException if {@code dest} is null
     * @see #cascadeCopyTo(JTransaction, String, int, boolean)
     */
    default JObject cascadeCopyTo(JTransaction dest, String cascadeName, boolean clone) {
        return this.cascadeCopyTo(dest, cascadeName, -1, clone);
    }

    /**
     * Copy this instance and all objects reachable from it via the specified cascade into the specified destination transaction.
     *
     * <p>
     * This is a more general method; see {@link #cascadeCopyIn cascadeCopyIn()} and {@link #cascadeCopyOut cascadeCopyOut()}
     * for more common and specific use cases.
     *
     * <p>
     * This method finds and copies all objects reachable from this object based on
     * {@link JField#forwardCascades &#64;JField.forwardCascades()} and
     * {@link JField#inverseCascades &#64;JField.inverseCascades()} annotation properties on
     * reference fields: a reference field is traversed in the forward or inverse direction if {@code cascadeName} is
     * specified in the corresponding annotation property. See {@link JField &#64;JField} for details.
     *
     * <p>
     * The {@code recursionLimit} parameter can be used to limit the maximum distance of any copied object,
     * measured in the number of reference field "hops" from this object.
     *
     * <p>
     * This instance will first be {@link #upgrade}ed if necessary. If any copied object already exists in {@code dest},
     * it will have its schema version updated first, if necessary, then be overwritten.
     * Any {@link OnVersionChange &#64;OnVersionChange}, {@link OnCreate &#64;OnCreate},
     * and {@link OnChange &#64;OnChange} methods will be notified accordingly as usual (in {@code dest}).
     *
     * <p>
     * The two transactions must be compatible in that for any schema versions encountered, those schema versions
     * must be identical in both transactions.
     *
     * <p>
     * Circular references are handled properly: if an object is encountered more than once, it is not copied again.
     * The {@code copyState} tracks which objects have already been copied and traversed.
     * For a "fresh" copy operation, pass a newly created {@link CopyState}; for a copy operation that is a continuation
     * of a previous copy, reuse the previous {@code copyState}. The {@link CopyState} may also be configured to remap object ID's.
     *
     * <p>
     * Warning: if two threads attempt to copy objects between the same two transactions at the same time
     * but in opposite directions, deadlock could result.
     *
     * @param dest destination transaction for copies
     * @param cascadeName cascade name, or null for no cascade (i.e., copy only this instance)
     * @param recursionLimit the maximum number of references to hop through, or -1 for infinity
     * @param clone true to clone objects, i.e., assign the copies new, unused object ID's in {@code dest},
     *  or false to preserve the same object ID's, overwriting any existing objects in {@code dest}
     * @return the copied version of this instance in {@code dest}
     * @throws DeletedObjectException if any object to be copied does not exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws SchemaMismatchException
     *  if the schema version corresponding to any copied object is not identical in both transactions
     * @throws IllegalArgumentException if {@code recursionLimit} is less that -1
     * @throws IllegalArgumentException if {@code dest} is null
     * @see #cascadeCopyIn cascadeCopyIn()
     * @see #cascadeCopyOut cascadeCopyOut()
     * @see JTransaction#cascadeFindAll JTransaction.cascadeFindAll()
     */
    default JObject cascadeCopyTo(JTransaction dest, String cascadeName, int recursionLimit, boolean clone) {
        Preconditions.checkArgument(dest != null, "null dest");
        final ObjId id = this.getObjId();
        final JTransaction jtx = this.getTransaction();
        final ObjIdSet ids = jtx.cascadeFindAll(id, cascadeName, recursionLimit);
        final CopyState copyState = clone ? new CopyState(dest.createClones(ids)) : new CopyState();
        jtx.copyTo(dest, copyState, ids);
        return dest.get(copyState.getDestinationId(id));
    }

    /**
     * Copy this instance and all objects reachable from it via the specified cascade
     * into the associated in-memory detached transaction.
     *
     * <p>
     * This method should only be invoked on regular database {@link JObject}s.
     * The returned object will always live in a {@link DetachedJTransaction}.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><pre>
     * this.cascadeCopyTo(this.getTransaction().getDetachedTransaction(), cascadeName, clone);
     * </pre></blockquote>
     *
     * @param cascadeName cascade name, or null for no cascade (i.e., copy only this instance)
     * @param clone true to clone objects, i.e., assign the copies new, unused object ID's in the detached transaction,
     *  or false to preserve the same object ID's, overwriting any existing objects
     * @return the detached {@link JObject} copy of this instance
     * @throws DeletedObjectException if any object to be copied does not exist
     * @throws SchemaMismatchException
     *  if the schema version corresponding to any copied object is not identical in both transactions
     * @throws IllegalArgumentException if this instance is a {@linkplain #isDetached detached instance}
     * @see #cascadeCopyIn cascadeCopyIn()
     * @see #cascadeCopyTo cascadeCopyTo()
     */
    default JObject cascadeCopyOut(String cascadeName, boolean clone) {
        return this.cascadeCopyTo(this.getTransaction().getDetachedTransaction(), cascadeName, clone);
    }

    /**
     * Copy this instance and all objects reachable from it via the specified cascade
     * into the transaction associated with the current thread.
     *
     * <p>
     * Normally this method would only be invoked on detached database {@link JObject}s.
     * The returned object will be a {@link JObject} in the currently open transaction.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><pre>
     * this.cascadeCopyTo(JTransaction.getCurrent(), cascadeName, clone);
     * </pre></blockquote>
     *
     * @param cascadeName cascade name, or null for no cascade (i.e., copy only this instance)
     * @param clone true to clone objects, i.e., assign the copies new, unused object ID's in the database transaction,
     *  or false to preserve the same object ID's, overwriting any existing objects
     * @return the regular database copy of this instance
     * @throws DeletedObjectException if any object to be copied does not exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field
     *  {@linkplain JField#allowDeleted configured} to disallow deleted assignment
     * @throws SchemaMismatchException
     *  if the schema version corresponding to any copied object is not identical in both transactions
     * @see #cascadeCopyOut cascadeCopyOut()
     * @see #cascadeCopyTo cascadeCopyTo()
     */
    default JObject cascadeCopyIn(String cascadeName, boolean clone) {
        return this.cascadeCopyTo(JTransaction.getCurrent(), cascadeName, clone);
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

    /**
     * Find all objects of the given type referring to this object through the specified reference field.
     *
     * <p>
     * The {@code fieldName} can be the name of a simple reference field (e.g., {@code "teacher"})
     * or a sub-field of a complex field (e.g., {@code "students.element"}).
     *
     * @param type type of referring objects
     * @param fieldName name of reference field
     * @param <R> type of referring objects
     * @return all objects of the specified type referring to this object through the named field
     * @throws StaleTransactionException if the transaction associated with this instance is no longer open
     * @throws IllegalArgumentException if either parameter is null
     */
    default <R> NavigableSet<R> findReferring(Class<R> type, String fieldName) {
        final NavigableSet<R> set = this.getTransaction().queryIndex(type, fieldName, Object.class).asMap().get(this);
        return set != null ? set : NavigableSets.empty();
    }

    /**
     * Get the {@link JClass} of which this {@link JObject} is an instance.
     *
     * @return associated {@link JClass}
     * @throws TypeNotInSchemaVersionException if this instance has a type that does not exist
     *  in this instance's schema version, i.e., this instance is an {@link UntypedJObject}
     */
    default JClass<?> getJClass() {
        return this.getTransaction().getPermazen().getJClass(this.getObjId());
    }

    /**
     * Get the original Java model class of which this {@link JObject} is an instance.
     *
     * @return Java model class
     */
    Class<?> getModelClass();
}
