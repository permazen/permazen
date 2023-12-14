
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when attempting to access an unknown type.
 */
@SuppressWarnings("serial")
public class UnknownTypeException extends DatabaseException {

    private final String typeName;
    private final Schema schema;

    /**
     * Constructor.
     *
     * @param typeName unknown type name
     * @param schema schema in which {@code typeName} was not found, or null if specific to any particular schema
     */
    public UnknownTypeException(String typeName, Schema schema) {
        this(typeName, schema, String.format(
          "no object type \"%s\" exists%s",
          typeName, schema != null ? String.format(" in schema \"%s\"", schema.getSchemaId()) : ""));
    }

    /**
     * Constructor.
     *
     * @param typeName unknown type name
     * @param schema schema in which {@code typeName} was not found, or null if specific to any particular schema
     * @param message exception message
     */
    public UnknownTypeException(String typeName, Schema schema, String message) {
        super(message);
        this.typeName = typeName;
        this.schema = schema;
    }

    /**
     * Get the type name that was not recognized.
     *
     * @return unrecognized object type name
     */
    public String getTypeName() {
        return this.typeName;
    }

    /**
     * Get the schema in which the type was not found.
     *
     * <p>
     * This may return null if a query was not specific to any particular schema.
     *
     * @return schema not having the object type, possibly null
     */
    public Schema getSchema() {
        return this.schema;
    }
}
