
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

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
     * @param description description of the unknown index
     */
    public UnknownIndexException(int storageId, String description) {
        super("no " + description + " with storage ID " + storageId + " exists");
        this.storageId = storageId;
    }

    /**
     * Get the storage ID that was not recognized.
     */
    public int getStorageId() {
        return this.storageId;
    }
}

