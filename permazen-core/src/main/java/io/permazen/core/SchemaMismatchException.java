
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.SchemaId;

/**
 * Thrown by {@link Database#createTransaction} when the expected schema does not match
 * and of the schemas already recorded in the database.
 */
@SuppressWarnings("serial")
public class SchemaMismatchException extends InvalidSchemaException {

    private final SchemaId schemaId;

    public SchemaMismatchException(SchemaId schemaId, String message) {
        super(message);
        this.schemaId = schemaId;
    }

    /**
     * Get the ID of the schema that failed to match.
     *
     * @return mismatched schema ID
     */
    public SchemaId getSchemaId() {
        return this.schemaId;
    }
}
