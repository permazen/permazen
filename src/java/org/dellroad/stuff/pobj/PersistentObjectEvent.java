
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.EventObject;

/**
 * Notification event emitted by a {@link PersistentObject} to listeners whenever there is an update to the root object.
 *
 * @param <T> type of the root persistent object
 */
@SuppressWarnings("serial")
public class PersistentObjectEvent<T> extends EventObject {

    private final long version;
    private final T oldRoot;
    private final T newRoot;

    public PersistentObjectEvent(PersistentObject<T> persistentObject, long version, T oldRoot, T newRoot) {
        super(persistentObject);
        this.version = version;
        this.oldRoot = oldRoot;
        this.newRoot = newRoot;
    }

    /**
     * Get the {@link PersistentObject} that originated this event.
     */
    @SuppressWarnings("unchecked")
    public PersistentObject<T> getSource() {
        return (PersistentObject<T>)super.getSource();
    }

    /**
     * Get the version that this event is associated with.
     * The {@link PersistentObject} class always delivers notifications in order, so this
     * number should always increase over time.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Get the old root prior to the update.
     */
    public T getOldRoot() {
        return this.oldRoot;
    }

    /**
     * Get the new root after to the update.
     */
    public T getNewRoot() {
        return this.newRoot;
    }
}

