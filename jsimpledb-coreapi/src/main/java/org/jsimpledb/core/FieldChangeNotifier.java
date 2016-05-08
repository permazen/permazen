
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

interface FieldChangeNotifier {

    /**
     * Get the storage ID of the field that chagned.
     */
    int getStorageId();

    /**
     * Get the ID of the object containing the field that chagned.
     */
    ObjId getId();

    /**
     * Notify the specified listener of the change.
     */
    void notify(Transaction tx, Object listener, int[] path, NavigableSet<ObjId> referrers);
}

