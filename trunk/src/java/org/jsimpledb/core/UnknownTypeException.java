
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Thrown when an unknown type is accessed.
 */
@SuppressWarnings("serial")
public class UnknownTypeException extends DatabaseException {

    private final int storageId;

    /**
     * Constructor.
     *
     * @param storageId unknown type storage ID
     */
    public UnknownTypeException(int storageId) {
        this.storageId = storageId;
    }

    /**
     * Constructor.
     *
     * @param storageId unknown type storage ID
     * @param message exception message
     */
    public UnknownTypeException(int storageId, String message) {
        super(message);
        this.storageId = storageId;
    }

    /**
     * Get the storage ID that was not recognized.
     */
    public int getStorageId() {
        return this.storageId;
    }
}

