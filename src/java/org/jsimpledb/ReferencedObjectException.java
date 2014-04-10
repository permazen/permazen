
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Thrown on an attempt to delete an object which is still referenced
 * by a {@link ReferenceField} configured for {@link DeleteAction#EXCEPTION}.
 */
@SuppressWarnings("serial")
public class ReferencedObjectException extends DatabaseException {

    private final ObjId id;
    private final ObjId referrer;
    private final int storageId;

    ReferencedObjectException(ObjId id, ObjId referrer, int storageId) {
        super("object " + id + " cannot be deleted because it is still referenced by field "
          + storageId + " in object " + referrer + ", which is configured for error on deletion");
        this.id = id;
        this.referrer = referrer;
        this.storageId = storageId;
    }

    /**
     * Get the ID of the object that could not be deleted because it was referenced.
     */
    public ObjId getId() {
        return this.id;
    }

    /**
     * Get the ID of the object that still refers to the object that was to be deleted.
     */
    public ObjId getReferrer() {
        return this.referrer;
    }

    /**
     * Get the storage ID of the field in the referring object that still referrs to the object that was supposed to be deleted.
     */
    public int getStorageId() {
        return this.storageId;
    }
}

