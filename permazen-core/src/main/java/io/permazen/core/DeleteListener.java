
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

/**
 * Listener interface for notifications that an object is about to be deleted.
 *
 * @see Transaction#addDeleteListener Transaction.addDeleteListener()
 */
@FunctionalInterface
public interface DeleteListener {

    /**
     * Receive notification of an object about to be deleted.
     *
     * <p>
     * Notifications are delivered in the same thread that is deleting the object, before the delete actually occurs.
     * At most one notification will be delivered for any object, even if {@link Transaction#delete Transaction.delete()}
     * is invoked reentrantly from within this listener.
     *
     * @param tx associated transaction
     * @param id the ID of the object about to be deleted
     * @param path path of reference fields (represented by storage IDs) that lead to {@code id}
     * @param referrers all objects that (indirectly) refer to the affected object via the {@code path}
     */
    void onDelete(Transaction tx, ObjId id, int[] path, NavigableSet<ObjId> referrers);
}
