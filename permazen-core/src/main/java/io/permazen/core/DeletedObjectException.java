
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when an object is accessed but the object does not exist in its associated transaction.
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
     * @throws NullPointerException if {@code tx} is null
     */
    public DeletedObjectException(Transaction tx, ObjId id) {
        this(id, "object with ID " + id + " (" + tx.getTypeDescription(id) + ") not found");
    }

    /**
     * Constructor.
     *
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
}
