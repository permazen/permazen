
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.Set;

class MapKeyStorageInfo<K> extends ComplexSubFieldStorageInfo<K, MapField<K, ?>> {

    MapKeyStorageInfo(MapField<K, ?> field) {
        super(field.keyField, field);
    }

    @Override
    void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix) {
        tx.readMapField(referrer, this.parentRepresentative.storageId, false).remove(target);
    }

    @Override
    void readAllNonNull(Transaction tx, ObjId target, Set<K> values) {
        for (K key : this.parentRepresentative.getValueInternal(tx, target).keySet()) {
            if (key != null)
                values.add(key);
        }
    }

// Object

    @Override
    public String toString() {
        return "map key field with " + this.fieldType;
    }
}

