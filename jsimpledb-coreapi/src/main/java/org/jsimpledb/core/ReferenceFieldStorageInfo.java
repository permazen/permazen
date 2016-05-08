
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

class ReferenceFieldStorageInfo extends SimpleFieldStorageInfo<ObjId> {

    ReferenceFieldStorageInfo(ReferenceField field, int superFieldStorageId) {
        super(field, FieldTypeRegistry.REFERENCE, superFieldStorageId);
    }

// Object

    @Override
    public String toString() {
        return "reference field";
    }

    @Override
    protected boolean fieldTypeEquals(SimpleFieldStorageInfo<?> that) {
        assert that instanceof ReferenceFieldStorageInfo;
        return true;        // reference fields are compatible even if they have different object type restriction lists
    }

    @Override
    protected int fieldTypeHashCode() {
        return 0;
    }
}

