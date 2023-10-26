
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

class SetElementStorageInfo<E> extends CollectionElementStorageInfo<NavigableSet<E>, E, SetField<E>> {

    SetElementStorageInfo(SetField<E> field) {
        super(field);
    }

    @Override
    void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix) {
        tx.readSetField(referrer, this.parentRepresentative.storageId, false).remove(target);
    }

// Object

    @Override
    public String toString() {
        return "set element with " + this.fieldType;
    }
}
