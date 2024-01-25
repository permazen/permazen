
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.OnCreate;
import io.permazen.annotation.OnSchemaChange;
import io.permazen.annotation.PermazenField;
import io.permazen.core.DeleteAction;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ObjId;
import io.permazen.core.ReferencedObjectException;
import io.permazen.core.SchemaMismatchException;
import io.permazen.core.StaleTransactionException;
import io.permazen.core.Transaction;
import io.permazen.core.TypeNotInSchemaException;
import io.permazen.core.UnknownTypeException;
import io.permazen.core.util.ObjIdSet;
import io.permazen.schema.SchemaId;

import jakarta.validation.groups.Default;

import java.util.Iterator;

/**
 * Interface implemented by {@link Permazen} Java model objects.
 *
 * <p>
 * All {@link Permazen} database objects are instances of runtime-generated sub-classes of user-provided Java model types.
 * These generated subclasses will always implement this interface, providing convenient access to database operations.
 * Therefore, it is convenient to declare Java model classes {@code abstract} and {@code implements PermazenObject}.
 * However, this is not strictly necessary; all of the methods declared here ultimately delegate to one of the
 * {@link PermazenTransaction} support methods.
 *
 * <p><b>Object Identity and State</b></p>
 *
 * <p>
 * Every {@link PermazenObject} has a unique 64-bit object identifier, represented as an {@link ObjId}.
 * All {@link PermazenObject} instances are permanently {@linkplain #getTransaction associated} with a specific
 * {@linkplain PermazenTransaction transaction}, and are the unique representatives for their corresponding {@link ObjId}
 * in that transaction. All field state derives from the associated transaction.
 *
 * <p>
 * There are two types of transactions: normal transactions reflecting an open transaction on the underlying
 * key/value database, and {@linkplain DetachedPermazenTransaction detached transactions}, which are in-memory containers
 * of object data. Detached transactions are fully functional, supporting index queries, object versioning, etc.
 *
 * <p><b>Copying Objects</b></p>
 *
 * <p>
 * This interface provides methods for copying a graph of objects between transactions, for example, from an open
 * key/value database transaction into an in-memory detached transaction ("copy out"), or vice-versa ("copy in").
 * A graph of objects can be copied by specifying a starting object and a list of reference cascades.
 * (see {@link PermazenField &#64;PermazenField}). Object ID's can be remapped during the copy if necessary, e.g., to ensure
 * existing objects are not overwritten (see {@link CopyState}).
 *
 * @see PermazenTransaction
 */
public interface PermazenObject {

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
     * Get this instance's associated {@link PermazenTransaction}.
     *
     * @return the {@link PermazenTransaction} that contains this instance's field state
     */
    PermazenTransaction getTransaction();

    /**
     * Get the {@link SchemaId} that identifies this instance's current schema.
     *
     * @return the ID of this instance's current schema
     * @throws DeletedObjectException
     *  if this object does not exist in the {@link PermazenTransaction} associated with this instance
     * @throws StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    default SchemaId getSchemaId() {
        return this.getTransaction().getTransaction().getObjType(this.getObjId()).getSchema().getSchemaId();
    }

    /**
     * Delete this instance, if it exists, in this instance's associated transaction.
     *
     * <p>
     * See {@link Transaction#delete Transaction.delete()} for details on secondary deletions from
     * {@link DeleteAction#DELETE} and {@link PermazenField#forwardDelete}.
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
     * with a {@link DetachedPermazenTransaction}.
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
     * The actual validation will occur either during {@link PermazenTransaction#commit}
     * or at the next invocation of {@link PermazenTransaction#validate}, whichever occurs first.
     * The specified validation groups, if any, will be used.
     *
     * <p>
     * If the associated transaction was opened with {@link ValidationMode#DISABLED}, no validation will be performed.
     *
     * @param groups validation group(s) to use for validation; if empty, {@link Default} is assumed
     * @throws DeletedObjectException
     *  if this object does not exist in the {@link PermazenTransaction} associated with this instance
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     * @throws IllegalArgumentException if {@code groups} is or any group in {@code groups} null
     */
    default void revalidate(Class<?>... groups) {
        this.getTransaction().revalidate(this.getObjId(), groups);
    }

    /**
     * Migrate the schema of this instance, if necessary, so that it matches the schema of its associated transaction.
     *
     * <p>
     * If a schema change occurs, {@link OnSchemaChange &#64;OnSchemaChange} methods will be invoked
     * prior to this method returning.
     *
     * @return true if the object's schema changed, false if it was already migrated
     * @throws DeletedObjectException
     *  if this object does not exist in the {@link PermazenTransaction} associated with this instance
     * @throws StaleTransactionException
     *  if the transaction {@linkplain #getTransaction associated with this instance} is no longer usable
     */
    default boolean migrateSchema() {
        return this.getTransaction().migrateSchema(this);
    }

    /**
     * Find all objects reachable through the specified reference cascade(s).
     *
     * <p>
     * This method finds all objects reachable from this instance through reference field cascades having the specified name(s).
     * In other words, a reference field is traversed in the {@linkplain io.permazen.annotation.PermazenField#forwardCascades
     * forward} or {@linkplain io.permazen.annotation.PermazenField#inverseCascades inverse} direction if one of the
     * {@code cascades} is specified in the corresponding {@linkplain io.permazen.annotation.PermazenField &#64;PermazenField}
     * annotation property.
     *
     * <p>
     * All objects found will be automatically {@linkplain #migrateSchema migrated} to this object's associated
     * transaction's schema.
     *
     * <p>
     * The {@code recursionLimit} parameter can be used to limit the maximum distance of any reachable object,
     * measured in the number of reference field "hops" from the given starting object.
     *
     * @param cascades zero or more reference cascades that identify additional objects to copy
     * @param recursionLimit the maximum number of reference fields to hop through, or -1 for infinity
     * @return the object ID's of the objects reachable from this object (including this object itself)
     * @throws DeletedObjectException if any object containing a traversed reference field does not actually exist
     * @throws IllegalArgumentException if {@code recursionLimit} is less that -1
     * @throws IllegalArgumentException if {@code cascades} or any element therein is null
     * @see PermazenTransaction#cascade PermazenTransaction.cascade()
     * @see io.permazen.annotation.PermazenField#forwardCascades &#64;PermazenField.forwardCascades()
     * @see io.permazen.annotation.PermazenField#inverseCascades &#64;PermazenField.inverseCascades()
     */
    default ObjIdSet cascade(int recursionLimit, String... cascades) {
        final ObjIdSet ids = new ObjIdSet();
        for (Iterator<ObjId> i = this.getTransaction().cascade(this.getObjId(), recursionLimit, ids, cascades); i.hasNext(); )
            i.next();
        return ids;
    }

    /**
     * Copy this instance, and other instances reachable through the specified reference cascades (if any),
     * into the in-memory, detached transaction associated with this instance's transaction.
     *
     * <p>
     * This method should only be invoked on regular database {@link PermazenObject}s.
     * The returned object will always live in a {@link DetachedPermazenTransaction}.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><pre>
     * this.copyTo(this.getTransaction().getDetachedTransaction(), -1, new CopyState(), cascades);
     * </pre></blockquote>
     *
     * @param cascades zero or more reference cascades that identify additional objects to copy
     * @return the detached {@link PermazenObject} copy of this instance
     * @throws SchemaMismatchException if the schema corresponding to this object's version is not identical in both transactions
     * @throws IllegalArgumentException if this instance is itself a {@linkplain #isDetached detached instance}
     * @throws IllegalArgumentException if {@code cascades} or any element therein is null
     * @see #copyIn copyIn()
     */
    default PermazenObject copyOut(String... cascades) {
        return this.copyTo(this.getTransaction().getDetachedTransaction(), -1, new CopyState(), cascades);
    }

    /**
     * Copy this instance, and other instances reachable through the specified reference cascades (if any),
     * into the transaction associated with the current thread.
     *
     * <p>
     * Normally this method would only be invoked on {@linkplain #isDetached detached} database objects.
     * The returned object will be a {@link PermazenObject} in the open transaction associated with the current thread.
     *
     * <p>
     * This is a convenience method, and is equivalent to invoking:
     * <blockquote><pre>
     * this.copyTo(PermazenTransaction.getCurrent(), -1, new CopyState(), cascades)
     * </pre></blockquote>
     *
     * @param cascades zero or more reference cascades that identify additional objects to copy
     * @return the regular database copy of this instance
     * @throws DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field {@linkplain PermazenField#allowDeleted configured}
     *  to disallow deleted assignment
     * @throws SchemaMismatchException if the schema corresponding to this object's version is not identical in both transactions
     * @throws IllegalArgumentException if {@code cascades} or any element therein is null
     * @see #copyOut copyOut()
     */
    default PermazenObject copyIn(String... cascades) {
        return this.copyTo(PermazenTransaction.getCurrent(), -1, new CopyState(), cascades);
    }

    /**
     * Copy this instance, and other instances it references through the specified reference cascades, into the
     * specified destination transaction.
     *
     * <p>
     * This is a more general method; see {@link #copyIn copyIn()} and {@link #copyOut copyOut()}
     * for more common and specific use cases.
     *
     * <p>
     * If a target object does not exist, it will be created, otherwise its schema will be migrated to match the source
     * object if necessary (with resulting {@link OnSchemaChange &#64;OnSchemaChange} notifications).
     * If {@link CopyState#isSuppressNotifications()} returns false, {@link OnCreate &#64;OnCreate}
     * and {@link OnChange &#64;OnChange} notifications will also be delivered.
     *
     * <p>
     * The {@code copyState} tracks which objects have already been copied. For a "fresh" copy operation,
     * pass a newly created {@link CopyState}; for a copy operation that is a continuation of a previous copy,
     * reuse the previous {@link CopyState}. The {@link CopyState} may also be configured to remap object ID's.
     *
     * <p>
     * This object's transaction and {@code dest} must be compatible in that for any schemas encountered, those schemas
     * must be identical in both.
     *
     * <p>
     * If {@code dest} is not a {@link DetachedPermazenTransaction} and any copied objects contain reference fields configured with
     * {@link io.permazen.annotation.PermazenField#allowDeleted}{@code = false}, then any objects referenced by those fields must
     * also be copied, or else must already exist in {@code dest}. Otherwise, a {@link DeletedObjectException} is thrown
     * and it is indeterminate which objects were copied.
     *
     * <p>
     * The {@code recursionLimit} parameter can be used to limit the maximum distance of any reachable object,
     * measured in the number of reference field "hops" from the given starting object.
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     *
     * @param dest destination transaction for copies
     * @param copyState tracks which objects have already been copied
     * @param recursionLimit the maximum number of reference fields to hop through, or -1 for infinity
     * @param cascades zero or more reference cascades that identify additional objects to copy
     * @return the copied version of this instance in {@code dest}
     * @throws DeletedObjectException if any object to be copied does not actually exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object
     *  that does not exist in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws SchemaMismatchException if the schema corresponding to any copied object is not identical in both the
     *  transaction associated with this instance and {@code dest}
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if any element in {@code cascades} is null
     * @see #copyIn copyIn()
     * @see #copyOut copyOut()
     * @see #cascade cascade()
     * @see PermazenTransaction#copyTo(PermazenTransaction, CopyState, Stream) PermazenTransaction.copyTo()
     */
    default PermazenObject copyTo(PermazenTransaction dest, int recursionLimit, CopyState copyState, String... cascades) {

        // Identify objects to copy
        final ObjIdSet ids = this.cascade(recursionLimit, cascades);

        // Copy objects
        this.getTransaction().copyTo(dest, copyState, ids);

        // Return the copy of this object in dest
        return dest.get(copyState.getDestId(this.getObjId()));
    }

    /**
     * Reset cached simple field values.
     *
     * <p>
     * {@link PermazenObject}s instances may cache simple field values after they have been read from the underlying
     * key/value store for efficiency. This method causes any such cached values to be forgotten, so they will
     * be re-read from the underlying key/value store on the next read of the field.
     *
     * <p>
     * Normally this method does not need to be used. It may be needed to maintain consistency
     * in exotic situations, for example, where the underlying key/value store is being modified directly.
     */
    void resetCachedFieldValues();

    /**
     * Get the {@link PermazenClass} of which this {@link PermazenObject} is an instance.
     *
     * @return associated {@link PermazenClass}
     * @throws TypeNotInSchemaException if this instance is an {@link UntypedPermazenObject}
     */
    default PermazenClass<?> getPermazenClass() {
        final ObjId id = this.getObjId();
        try {
            return this.getTransaction().getPermazen().getPermazenClass(id);
        } catch (UnknownTypeException e) {
            throw (TypeNotInSchemaException)new TypeNotInSchemaException(id, "storage ID " + id.getStorageId(), null).initCause(e);
        }
    }

    /**
     * Get the original Java model class of which this {@link PermazenObject} is an instance.
     *
     * @return Java model class
     */
    Class<?> getModelClass();
}
