
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.SortedMap;
import java.util.TreeMap;

class ObjTypeStorageInfo extends StorageInfo {

    final TreeMap<Integer, FieldStorageInfo> fields = new TreeMap<>();

    ObjTypeStorageInfo(ObjType objType) {
        super(objType.storageId);
    }

    /**
     * Get the fields associated with this instance.
     */
    public SortedMap<Integer, FieldStorageInfo> getFields() {
        return this.fields;
    }

    @Override
    public String toString() {
        return "object type";
    }
}

