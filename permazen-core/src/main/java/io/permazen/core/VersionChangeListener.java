
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.Map;

/**
 * Listener interface for notifications that an object's schema version has been changed to match the
 * current transaction.
 *
 * @see Transaction#addVersionChangeListener Transaction.addVersionChangeListener()
 */
@FunctionalInterface
public interface VersionChangeListener {

    /**
     * Receive notification of an object schema version change.
     *
     * <p>
     * Notifications are delivered in the same thread that first reads the object, before the operation
     * that triggered the schema version change returns.
     *
     * @param tx associated transaction
     * @param id the ID of the updated object
     * @param oldVersion previous object schema version
     * @param newVersion new object schema version
     * @param oldFieldValues read-only mapping of the values of all fields in the old schema version keyed by storage ID
     */
    void onVersionChange(Transaction tx, ObjId id, int oldVersion, int newVersion, Map<Integer, Object> oldFieldValues);
}

