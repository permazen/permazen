
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.Comparator;

/**
 * Class that holds information about the {@link SchemaItem}s associated with a storage ID,
 * independent of any specific schema version. This is exactly the information that must
 * be consistent across schema versions.
 *
 * @see #canShareStorageId canShareStorageId()
 */
abstract class StorageInfo {

    public static final Comparator<StorageInfo> SORT_BY_STORAGE_ID = new Comparator<StorageInfo>() {
        @Override
        public int compare(StorageInfo info1, StorageInfo info2) {
            return Integer.compare(info1.getStorageId(), info2.getStorageId());
        }
    };

    final int storageId;

    StorageInfo(int storageId) {
        if (storageId <= 0)
            throw new IllegalArgumentException("invalid storageId " + storageId);
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

    /**
     * Compare for compatability across schema versions.
     *
     * <p>
     * Subclasses must override as required.
     * </p>
     */
    public boolean canShareStorageId(StorageInfo that) {
        return this.getClass() == that.getClass() && this.storageId == that.storageId;
    }

    @Override
    public abstract String toString();
}

