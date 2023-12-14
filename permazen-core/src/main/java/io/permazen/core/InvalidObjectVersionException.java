
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when an object is found to have an invalid schema index.
 *
 * <p>
 * This indicates database or recorded schema corruption.
 */
@SuppressWarnings("serial")
public class InvalidObjectVersionException extends InconsistentDatabaseException {

    private final ObjId id;
    private final int schemaIndex;

    InvalidObjectVersionException(ObjId id, int schemaIndex, Throwable cause) {
        super(String.format("object %s has an invalid schema index %d", id, schemaIndex), cause);
        this.id = id;
        this.schemaIndex = schemaIndex;
    }

    /**
     * Get the ID of the object containing the invalid schema index.
     *
     * @return object ID
     */
    public ObjId getId() {
        return this.id;
    }

    /**
     * Get the invalid schema index number found.
     *
     * @return invalid schema index
     */
    public int getSchemaIndex() {
        return this.schemaIndex;
    }
}
