
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
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

    /**
     * Constructor.
     *
     * @param persistentObject source of this event
     * @param version the new persistent object version (i.e., the version of {@code newRoot})
     * @param oldRoot previous root object; null if exiting from an empty start period
     * @param newRoot updated root object; null if entering an empty stop period
     */
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
     * Get the version that this event is associated with. This will be the version of the {@linkplain #getNewRoot new root}.
     *
     * <p>
     * The {@link PersistentObject} class always delivers notifications in order, so this
     * number should always increase over time.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Get the old root prior to the update.
     *
     * <p>
     * The caller must not modify the returned object, as it is shared among all listeners.
     *
     * @return the old root object; will be null if an empty start period has just ended
     */
    public T getOldRoot() {
        return this.oldRoot;
    }

    /**
     * Get the new root after the update.
     *
     * <p>
     * The caller must not modify the returned object, as it is shared among all listeners.
     *
     * @return the new root object; will be null if an empty stop period has just started
     */
    public T getNewRoot() {
        return this.newRoot;
    }
}

