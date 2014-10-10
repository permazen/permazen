
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

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final FieldStorageInfo that = (FieldStorageInfo)obj;
        return this.superFieldStorageId == that.superFieldStorageId;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.superFieldStorageId;
    }
}

