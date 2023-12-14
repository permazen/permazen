
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when attempting to migrate a database object to a new schema in which the object's type no longer exists.
 *
 * @see Transaction#migrateSchema Transaction.migrateSchema()
 */
@SuppressWarnings("serial")
public class TypeNotInSchemaException extends UnknownTypeException {

    private final ObjId id;

    /**
     * Constructor.
     *
     * @param id ID for the object whose type was not known
     * @param typeName object's type name
     * @param schema schema in which {@code typeName} was not found
     */
    public TypeNotInSchemaException(ObjId id, String typeName, Schema schema) {
        super(typeName, schema);
        this.id = id;
    }

    /**
     * Get the {@link ObjId} of the object that could not be migrated.
     *
     * @return unmigrated object ID
     */
    public ObjId getObjId() {
        return this.id;
    }
}
