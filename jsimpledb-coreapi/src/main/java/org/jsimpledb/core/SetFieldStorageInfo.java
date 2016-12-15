
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

class SetFieldStorageInfo<E> extends CollectionFieldStorageInfo<NavigableSet<E>, E> {

    SetFieldStorageInfo(SetField<E> field) {
        super(field);
    }

    @Override
    CoreIndex<E, ObjId> getElementFieldIndex(Transaction tx) {
        return new CoreIndex<>(tx,
          new IndexView<>(this.elementField.storageId, this.elementField.fieldType, FieldTypeRegistry.OBJ_ID));
    }

    @Override
    CoreIndex<E, ObjId> getSimpleSubFieldIndex(Transaction tx, SimpleFieldStorageInfo<?> subField) {
        assert subField.equals(this.elementField);
        return this.getElementFieldIndex(tx);
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

