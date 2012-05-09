
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

/**
 * Listener interface for changes to a {@link PersistentObject}.
 *
 * <p>
 * Listener notifications are delivered in a separate thread from the one that caused the root object to change.
 *
 * @param <T> type of the root persistent object
 */
public interface PersistentObjectListener<T> {

    /**
     * Handle notification of an updated root object.
     */
    void handleEvent(PersistentObjectEvent<T> event);
}

