
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
 * {@link JSimpleDB} automatically generates sub-classes of user-supplied Java model classes.
 * These sub-classes know how to access database fields in the transaction associated with the current thread.
 * In addition, these sub-classes implement this interface.
 * </p>
 */
public interface JObject {

    /**
     * Get this instance's unique JSimpleDB object identifier.
     *
     * <p>
     * This method will always succeed even if there is no transaction associated with the current thread.
     * </p>
     *
     * @return unique database identifier for this instance
     */
    ObjId getObjId();

    /**
     * Delete this instance, if it exists, in
     * {@linkplain JTransaction#getCurrent the transaction associated with the current thread}.
     *
     * @return true if instance was deleted, false if it did not exist
     * @throws IllegalStateException if there is no {@link JTransaction} associated with the current thread
     * @throws org.jsimpledb.StaleTransactionException
     *  if the transaction associated with the current thread is no longer usable
     */
    boolean delete();

    /**
     * Determine whether this instance still exists in
     * {@linkplain JTransaction#getCurrent the transaction associated with the current thread}.
     *
     * @return true if instance exists, otherwise false
     * @throws IllegalStateException if there is no {@link JTransaction} associated with the current thread
     * @throws org.jsimpledb.StaleTransactionException
     *  if the transaction associated with the current thread is no longer usable
     */
    boolean exists();

    /**
     * Recreate a deleted instance, if it does not exist, in
     * {@linkplain JTransaction#getCurrent the transaction associated with the current thread}.
     * The fields of a recreated object will be set to their initial values.
     *
     * @return true if instance was recreated, false if it already existed
     * @throws IllegalStateException if there is no {@link JTransaction} associated with the current thread
     * @throws org.jsimpledb.StaleTransactionException
     *  if the transaction associated with the current thread is no longer usable
     */
    boolean recreate();

    /**
     * Add this instance to the validation queue for validation in
     * {@linkplain JTransaction#getCurrent the transaction associated with the current thread}.
     *
     * <p>
     * The actual validation will occur either during {@link JTransaction#commit}
     * or at the next invocation of {@link JTransaction#validate}, whichever occurs first.
     * </p>
     *
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws IllegalStateException if there is no {@link JTransaction} associated with the current thread
     * @throws org.jsimpledb.StaleTransactionException
     *  if the transaction associated with the current thread is no longer usable
     */
    void revalidate();

    /**
     * Update the schema version of this instance, if necessary, so that it matches the schema version of the
     * {@linkplain JTransaction#getCurrent the transaction associated with the current thread}.
     *
     * <p>
     * If a version change occurs, matching {@link OnVersionChange &#64;OnVersionChange} methods will be invoked prior
     * to this method returning.
     * </p>
     *
     * @throws IllegalStateException if there is no {@link JTransaction} associated with the current thread
     * @throws org.jsimpledb.StaleTransactionException
     *  if the transaction associated with the current thread is no longer usable
     */
    void upgrade();
}

