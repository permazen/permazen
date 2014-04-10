
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

abstract class FieldStorageInfo extends StorageInfo {

    final int superFieldStorageId;

    FieldStorageInfo(Field<?> field, int superFieldStorageId) {
        super(field.storageId);
        this.superFieldStorageId = superFieldStorageId;
    }

    /**
     * Get whether the associated field is a sub-field.
     */
    public boolean isSubField() {
        return this.superFieldStorageId != 0;
    }

    @Override
    public boolean canShareStorageId(StorageInfo obj) {
        if (!super.canShareStorageId(obj))
            return false;
        final FieldStorageInfo that = (FieldStorageInfo)obj;
        return this.superFieldStorageId == that.superFieldStorageId;
    }
}

