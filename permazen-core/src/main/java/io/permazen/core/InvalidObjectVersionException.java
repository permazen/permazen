
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when an object is found to have an invalid schema version.
 * This indicates database or recorded schema corruption.
 */
@SuppressWarnings("serial")
public class InvalidObjectVersionException extends InconsistentDatabaseException {

    private final ObjId id;
    private final int version;

    InvalidObjectVersionException(ObjId id, int version) {
        super("object " + id + " contains invalid schema version number "
          + version + " which is not recorded in the database");
        this.id = id;
        this.version = version;
    }

    /**
     * Get the ID of the object containing the invalid version number.
     *
     * @return object ID
     */
    public ObjId getId() {
        return this.id;
    }

    /**
     * Get the invalid version number found.
     *
     * @return invalid version number
     */
    public int getInvalidVersion() {
        return this.version;
    }
}
