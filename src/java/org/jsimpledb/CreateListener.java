
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Listener interface for notifications that an object has just been created.
 *
 * @see Transaction#addCreateListener Transaction.addCreateListener()
 */
public interface CreateListener {

    /**
     * Receive notification of a new object being created.
     *
     * <p>
     * Notifications are delivered in the same thread that is creating object, immediately after creation.
     * </p>
     *
     * @param tx associated transaction
     * @param id the ID of the new object
     */
    void onCreate(Transaction tx, ObjId id);
}

