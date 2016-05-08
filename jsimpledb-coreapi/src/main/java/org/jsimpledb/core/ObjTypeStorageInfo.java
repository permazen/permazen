
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

class ObjTypeStorageInfo extends StorageInfo {

    ObjTypeStorageInfo(ObjType objType) {
        super(objType.storageId);
    }

// Object

    @Override
    public String toString() {
        return "object type";
    }
}

