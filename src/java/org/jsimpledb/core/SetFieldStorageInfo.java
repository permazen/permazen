
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.jsimpledb.util.ByteReader;

class SetFieldStorageInfo extends CollectionFieldStorageInfo {

    SetFieldStorageInfo(SetField<?> field) {
        super(field);
    }

    @Override
    void unreference(Transaction tx, int storageId, ObjId target, ObjId referrer, ByteReader reader) {
        tx.readSetField(referrer, this.storageId).remove(target);
    }

    @Override
    public String toString() {
        return "set field with element " + this.elementField;
    }
}

