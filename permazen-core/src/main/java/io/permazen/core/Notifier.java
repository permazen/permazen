
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

/**
 * Notifies about some change in an object found through a path of references.
 *
 * @param <L> the listener type that is the target of the notification
 */
abstract class Notifier<L> {

    protected final ObjId id;

    /**
     * Constructor.
     *
     * @param id notifying object
     */
    Notifier(ObjId id) {
        assert id != null;
        this.id = id;
    }

    /**
     * Get the listener type.
     */
    public abstract Class<L> getListenerType();

    /**
     * Notify the specified listener.
     */
    public abstract void notify(Transaction tx, L listener, int[] path, NavigableSet<ObjId> referrers);
}
