
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

abstract class FieldStorageInfo extends StorageInfo {

    FieldStorageInfo(Field<?> field) {
        super(field.storageId);
    }

    /**
     * Get whether the associated field is a sub-field.
     */
    public boolean isSubField() {
        return false;
    }
}

