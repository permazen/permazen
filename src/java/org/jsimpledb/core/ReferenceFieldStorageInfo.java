
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

class ReferenceFieldStorageInfo extends SimpleFieldStorageInfo {

    ReferenceFieldStorageInfo(ReferenceField field, int superFieldStorageId) {
        super(field, superFieldStorageId);
    }

// Object

    @Override
    public String toString() {
        return "reference field";
    }

    @Override
    protected boolean fieldTypeEquals(SimpleFieldStorageInfo that) {
        return true;        // reference fields are compatible even if they have different object type restriction lists
    }

    @Override
    protected int fieldTypeHashCode() {
        return 0;
    }
}

