
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Thrown when an object is found to have an invalid schema version.
 * This indicates database or recorded schema corruption.
 */
@SuppressWarnings("serial")
public class InvalidObjectVersionException extends InconsistentDatabaseException {

    private final ObjId id;
    private final int versionNumber;

    InvalidObjectVersionException(ObjId id, int versionNumber) {
        super("object " + id + " contains invalid schema version number "
          + versionNumber + " which is not recorded in the database");
        this.id = id;
        this.versionNumber = versionNumber;
    }

    /**
     * Get the ID of the object containing the invalid version number.
     */
    public ObjId getId() {
        return this.id;
    }

    /**
     * Get the invalid version number found.
     */
    public int getVersionNumber() {
        return this.versionNumber;
    }
}

