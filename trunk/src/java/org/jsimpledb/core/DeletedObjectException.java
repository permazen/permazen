
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Thrown when a field of a deleted object is accessed.
 */
@SuppressWarnings("serial")
public class DeletedObjectException extends DatabaseException {

    private final ObjId id;

    /**
     * Constructor.
     *
     * @param id the ID of the object that was not found
     */
    public DeletedObjectException(ObjId id) {
        super("object with ID " + id + " not found");
        this.id = id;
    }

    /**
     * Get the ID of the object that could not be accessed.
     *
     * @return deleted object's ID
     */
    public ObjId getId() {
        return this.id;
    }
}

