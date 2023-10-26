
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when an unknown index is accessed.
 */
@SuppressWarnings("serial")
public class UnknownIndexException extends DatabaseException {

    private final int storageId;

    /**
     * Constructor.
     *
     * @param storageId unknown index storage ID
     * @param description description of the problem
     */
    public UnknownIndexException(int storageId, String description) {
        super(description);
        this.storageId = storageId;
    }

    /**
     * Get the storage ID that was not recognized.
     *
     * @return unrecognized index storage ID
     */
    public int getStorageId() {
        return this.storageId;
    }
}
