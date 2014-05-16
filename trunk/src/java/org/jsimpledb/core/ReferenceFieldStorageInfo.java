
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

class ReferenceFieldStorageInfo extends SimpleFieldStorageInfo {

    ReferenceFieldStorageInfo(ReferenceField field, int superFieldStorageId, boolean hasComplexIndex) {
        super(field, superFieldStorageId, hasComplexIndex);
    }

    @Override
    public String toString() {
        return "reference field";
    }
}

