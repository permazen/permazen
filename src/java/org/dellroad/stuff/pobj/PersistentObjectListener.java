
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

/**
 * Listener interface for changes to a {@link PersistentObject}.
 *
 * @param <T> type of the root persistent object
 */
public interface PersistentObjectListener<T> {

    /**
     * Handle notification of an updated root object.
     */
    void handleEvent(PersistentObjectEvent<T> event);
}

