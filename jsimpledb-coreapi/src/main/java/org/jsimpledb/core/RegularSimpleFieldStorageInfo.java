
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

/**
 * Represents an index on a regular simple field (not a sub-field of a complex field).
 */
class RegularSimpleFieldStorageInfo<T> extends SimpleFieldStorageInfo<T> {

    RegularSimpleFieldStorageInfo(SimpleField<T> field) {
        super(field);
        assert field.parent == null;
    }

    @Override
    void unreferenceAll(Transaction tx, ObjId target, NavigableSet<ObjId> referrers) {
        assert this.fieldType instanceof ReferenceFieldType;
        for (ObjId referrer : referrers)
            tx.writeSimpleField(referrer, this.storageId, null, false);
    }

// Object

    @Override
    public String toString() {
        return "simple field with " + this.fieldType;
    }
}

