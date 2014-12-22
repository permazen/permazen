
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

class SetFieldStorageInfo<E> extends CollectionFieldStorageInfo<NavigableSet<E>, E> {

    SetFieldStorageInfo(SetField<E> field) {
        super(field);
    }

    @Override
    void unreference(Transaction tx, int storageId, ObjId target, ObjId referrer, byte[] prefix) {
        assert storageId == this.elementField.storageId;
        tx.readSetField(referrer, this.storageId, false).remove(target);
    }

    @Override
    public String toString() {
        return "set field with element " + this.elementField;
    }
}

