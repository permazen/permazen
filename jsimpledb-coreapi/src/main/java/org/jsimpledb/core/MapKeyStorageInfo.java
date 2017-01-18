
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

class MapKeyStorageInfo<K> extends ComplexSubFieldStorageInfo<K> {

    MapKeyStorageInfo(MapField<K, ?> field) {
        super(field.keyField);
    }

    @Override
    void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix) {
        tx.readMapField(referrer, this.parentStorageId, false).remove(target);
    }

// Object

    @Override
    public String toString() {
        return "map key field with " + this.fieldType;
    }
}

