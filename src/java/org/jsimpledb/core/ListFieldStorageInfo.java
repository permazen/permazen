
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.UnsignedIntEncoder;

class ListFieldStorageInfo extends CollectionFieldStorageInfo {

    ListFieldStorageInfo(ListField<?> field) {
        super(field);
    }

    @Override
    void unreference(Transaction tx, int storageId, ObjId target, ObjId referrer, ByteReader reader) {
        tx.readListField(referrer, this.storageId).remove(UnsignedIntEncoder.read(reader));
    }

    @Override
    public String toString() {
        return "list field with element " + this.elementField;
    }
}

