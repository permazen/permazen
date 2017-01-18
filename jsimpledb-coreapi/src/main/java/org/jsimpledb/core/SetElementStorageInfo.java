
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

class SetElementStorageInfo<E> extends ComplexSubFieldStorageInfo<E> {

    SetElementStorageInfo(SetField<E> field) {
        super(field.elementField);
    }

    @Override
    void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix) {
        tx.readSetField(referrer, this.parentStorageId, false).remove(target);
    }

// Object

    @Override
    public String toString() {
        return "set element with " + this.fieldType;
    }
}

