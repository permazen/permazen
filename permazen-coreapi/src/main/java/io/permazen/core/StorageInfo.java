
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

/**
 * Class that describes what information is stored under a storage ID.
 *
 * <p>
 * This information is schema-wide and independent of any particular field, etc. The {@link StorageInfo}'s
 * associated with any particular storage ID in a schema, or between schemas, must be identical.
 * This ensures we don't have conflicting data encoded under the same key prefix.
 */
abstract class StorageInfo {

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

