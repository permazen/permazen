
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Listener interface for notifications that an object is about to be deleted.
 *
 * @see Transaction#addDeleteListener Transaction.addDeleteListener()
 */
@FunctionalInterface
public interface DeleteListener {

    /**
     * Receive notification of an object being deleted.
     *
     * <p>
     * Notifications are delivered in the same thread that is deleting object, before the delete actually occurs.
     * At most one notification will be delivered for any object, even if {@link Transaction#delete Transaction.delete()}
     * is invoked reentrantly from within this listener.
     *
     * @param tx associated transaction
     * @param id the ID of the object about to be deleted
     */
    void onDelete(Transaction tx, ObjId id);
}

