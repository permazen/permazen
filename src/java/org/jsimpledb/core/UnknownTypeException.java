
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

/**
 * Thrown when attempting to access an unknown type.
 */
@SuppressWarnings("serial")
public class UnknownTypeException extends DatabaseException {

    private final int storageId;
    private final int schemaVersion;

    /**
     * Constructor.
     *
     * @param storageId unknown type storage ID
     * @param schemaVersion schema version in which type was not found, or zero if not version specific
     */
    public UnknownTypeException(int storageId, int schemaVersion) {
        this(storageId, schemaVersion, "no object type with storage ID " + storageId + " exists"
          + (schemaVersion != 0 ? " in schema version " + schemaVersion : ""));
    }

    /**
     * Constructor.
     *
     * @param storageId unknown type storage ID
     * @param schemaVersion schema version in which type was not found, or zero if not version specific
     * @param message exception message
     */
    public UnknownTypeException(int storageId, int schemaVersion, String message) {
        super(message);
        this.storageId = storageId;
        this.schemaVersion = schemaVersion;
    }

    /**
     * Get the storage ID that was not recognized.
     *
     * @return unrecognized object type storage ID
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Get the schema version in which the type was not found.
     * This may return zero if a query was not specific to a single schema version.
     *
     * @return schema version not having the object type
     */
    public int getSchemaVersion() {
        return this.schemaVersion;
    }
}

