
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Thrown when attempting to update a database object to a schema version in which the object's type is not defined.
 *
 * @see Transaction#updateSchemaVersion Transaction.updateSchemaVersion()
 */
@SuppressWarnings("serial")
public class TypeNotInSchemaVersionException extends DatabaseException {

    private final ObjId id;
    private final int version;

    /**
     * Constructor.
     */
    public TypeNotInSchemaVersionException(ObjId id, int version) {
        this(id, version, "no object type with storage ID " + id.getStorageId() + " exists in database schema version " + version);
    }

    /**
     * Constructor.
     */
    public TypeNotInSchemaVersionException(ObjId id, int version, String message) {
        super(message);
        this.id = id;
        this.version = version;
    }

    /**
     * Get the {@link ObjId} of the object that could not be upgraded to its unrecognized type.
     */
    public ObjId getObjId() {
        return this.id;
    }

    /**
     * Get the schema version which does not contain the object's type.
     */
    public int getSchemaVersion() {
        return this.version;
    }
}

