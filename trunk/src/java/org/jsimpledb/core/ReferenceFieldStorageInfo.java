
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

    @Override
    public String toString() {
        return "reference field";
    }
}

