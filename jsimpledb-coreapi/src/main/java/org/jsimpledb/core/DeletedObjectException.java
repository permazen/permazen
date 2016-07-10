
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
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
        this(id, "object with ID " + id + " not found");
    }

    /**
     * Constructor.
     *
     * @param tx the transaction within which this exception was generated
     * @param id the ID of the object that was not found
     */
    public DeletedObjectException(Transaction tx, ObjId id) {
        this(id, "object with ID " + id + " (" + DeletedObjectException.getTypeDescription(tx, id) + ") not found");
    }

    /**
     * Constructor.
     *
     * @param tx the transaction within which this exception was generated
     * @param id the ID of the object that was not found
     * @param message detail message
     */
    public DeletedObjectException(ObjId id, String message) {
        super(message);
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

    static String getTypeDescription(Transaction tx, ObjId id) {
        final int storageId = id.getStorageId();
        final ObjType type = tx != null ? tx.schema.objTypeMap.get(id.getStorageId()) : null;
        if (type == null)
            return "type having storage ID " + storageId;
        return "type `" + type.getName() + "' having storage ID " + storageId;
    }
}

