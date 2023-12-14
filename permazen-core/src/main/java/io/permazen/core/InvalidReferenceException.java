
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import java.util.Set;

/**
 * Thrown when attempting to set a reference field to a disallowed object type.
 *
 * @see ReferenceField#getObjectTypes
 */
@SuppressWarnings("serial")
public class InvalidReferenceException extends IllegalArgumentException {

    private final ObjId id;
    private final String typeName;
    private final Set<String> objectTypes;

    /**
     * Constructor.
     *
     * @param id offending object's ID
     * @param typeName offending object's type
     * @param objectTypes allowed object types
     * @throws IllegalArgumentException if any parameter is null
     */
    public InvalidReferenceException(ObjId id, String typeName, Set<String> objectTypes) {
        super(String.format("illegal reference %s to object type \"%s\": allowed object types are %s", id, typeName, objectTypes));
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(typeName != null, "null typeName");
        Preconditions.checkArgument(objectTypes != null, "null objectTypes");
        this.id = id;
        this.typeName = typeName;
        this.objectTypes = objectTypes;
    }

    /**
     * Get the {@link ObjId} of the object that could not be assigned to the reference field due to disallowed object type.
     *
     * @return the object having disallowed type
     */
    public ObjId getObjId() {
        return this.id;
    }

    /**
     * Get the name of the object type that could not be assigned to the reference field due to disallowed object type.
     *
     * @return offending type name
     */
    public String getTypeName() {
        return this.typeName;
    }

    /**
     * Get the set of object types that are allowed to be referenced by the reference field.
     *
     * @return set of allowed object type names
     */
    public Set<String> getObjectTypes() {
        return this.objectTypes;
    }
}
