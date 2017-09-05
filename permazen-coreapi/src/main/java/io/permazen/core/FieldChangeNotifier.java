
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

abstract class FieldChangeNotifier<T> {

    protected final Class<T> listenerType;
    protected final int storageId;
    protected final ObjId id;

    FieldChangeNotifier(Class<T> listenerType, int storageId, ObjId id) {
        assert listenerType != null;
        assert storageId > 0;
        assert id != null;
        this.listenerType = listenerType;
        this.storageId = storageId;
        this.id = id;
    }

    /**
     * Get the required listener type.
     */
    public Class<T> getListenerType() {
        return this.listenerType;
    }

    /**
     * Get the storage ID of the field that chagned.
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Get the ID of the object containing the field that chagned.
     */
    public ObjId getId() {
        return this.id;
    }

    /**
     * Notify the specified listener of the change.
     */
    public abstract void notify(Transaction tx, T listener, int[] path, NavigableSet<ObjId> referrers);
}
