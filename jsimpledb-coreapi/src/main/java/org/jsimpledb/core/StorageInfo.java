
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;

import java.util.Comparator;

/**
 * Class that holds information about the {@link SchemaItem}s associated with a storage ID,
 * independent of any specific schema version. This is exactly the information that must
 * be consistent across schema versions.
 */
abstract class StorageInfo {

    public static final Comparator<StorageInfo> SORT_BY_STORAGE_ID = Comparator.comparingInt(i -> i.storageId);

    final int storageId;

    StorageInfo(int storageId) {
        Preconditions.checkArgument(storageId > 0, "invalid non-positive storageId");
        this.storageId = storageId;
    }

    /**
     * Get the storage ID associated with this instance.
     *
     * @return storage ID, always greater than zero
     */
    public int getStorageId() {
        return this.storageId;
    }

// Object

    @Override
    public abstract String toString();

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final StorageInfo that = (StorageInfo)obj;
        return this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return this.storageId;
    }
}

