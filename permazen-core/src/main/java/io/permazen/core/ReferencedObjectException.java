
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown on an attempt to delete an object which is still referenced
 * by a {@link ReferenceField} configured for {@link DeleteAction#EXCEPTION}.
 */
@SuppressWarnings("serial")
public class ReferencedObjectException extends DatabaseException {

    private final ObjId id;
    private final ObjId referrer;
    private final int storageId;

    ReferencedObjectException(Transaction tx, ObjId id, ObjId referrer, int storageId) {
        super(tx.getObjDescription(id) + " cannot be deleted because it is still referenced by "
          + tx.getFieldDescription(referrer, storageId) + " in " + tx.getObjDescription(referrer)
          + ", which is configured for error on deletion");
        this.id = id;
        this.referrer = referrer;
        this.storageId = storageId;
    }

    /**
     * Get the ID of the object that could not be deleted because it was referenced.
     *
     * @return referenced object's ID
     */
    public ObjId getId() {
        return this.id;
    }

    /**
     * Get the ID of the object that still refers to the object that was to be deleted.
     *
     * @return referencing object's ID
     */
    public ObjId getReferrer() {
        return this.referrer;
    }

    /**
     * Get the storage ID of the field in the referring object that still referrs to the object that was supposed to be deleted.
     *
     * @return reference field storage ID
     */
    public int getStorageId() {
        return this.storageId;
    }
}
