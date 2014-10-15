
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Set;

/**
 * Thrown when attempting to set a reference field to a disallowed object type.
 *
 * @see ReferenceField#getObjectTypes
 */
@SuppressWarnings("serial")
public class InvalidReferenceException extends IllegalArgumentException {

    private final ObjId id;
    private final Set<Integer> objectTypes;

    /**
     * Constructor.
     */
    public InvalidReferenceException(ObjId id, Set<Integer> objectTypes) {
        super("illegal reference " + id + " with object type #" + id.getStorageId() + "; allowed object types are " + objectTypes);
        this.id = id;
        this.objectTypes = objectTypes;
    }

    /**
     * Constructor.
     */
    public InvalidReferenceException(ReferenceField field, ObjId id) {
        this(id, field.getObjectTypes());
    }

    /**
     * Get the {@link ObjId} of the object that could not be assigned to the reference field due to disallowed object type.
     */
    public ObjId getObjId() {
        return this.id;
    }

    /**
     * Get the set of object types that are allowed to be referenced by the reference field.
     *
     * @return set of allowed object type storage IDs
     */
    public Set<Integer> getObjectTypes() {
        return this.objectTypes;
    }
}

