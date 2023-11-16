
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

/**
 * Support superclass for information about an index.
 */
abstract class IndexInfo {

    final int storageId;

    IndexInfo(int storageId) {
        this.storageId = storageId;
    }

    /**
     * Get index storage info.
     *
     * @return index storage ID
     */
    public int getStorageId() {
        return this.storageId;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final IndexInfo that = (IndexInfo)obj;
        return this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.storageId;
    }
}
