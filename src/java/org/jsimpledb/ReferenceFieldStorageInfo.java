
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

class ReferenceFieldStorageInfo extends SimpleFieldStorageInfo {

    final DeleteAction onDelete;

    ReferenceFieldStorageInfo(ReferenceField field, int superFieldStorageId) {
        super(field, superFieldStorageId);
        this.onDelete = field.onDelete;
    }

    /**
     * Get the {@link DeleteAction} associated with the {@link ReferenceField}.
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }

    @Override
    public boolean canShareStorageId(StorageInfo obj) {
        if (!super.canShareStorageId(obj))
            return false;
        final ReferenceFieldStorageInfo that = (ReferenceFieldStorageInfo)obj;
        return this.onDelete == that.onDelete;
    }

    @Override
    public String toString() {
        return "reference field with onDelete " + this.onDelete;
    }
}

