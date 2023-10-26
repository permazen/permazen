
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown by {@link Database#createTransaction} when the given schema cannot be matched with the
 * schema of the same version already recorded in the database, or cannot be added to the database
 * because it is incompatible with one or more prior schema versions.
 */
@SuppressWarnings("serial")
public class SchemaMismatchException extends InvalidSchemaException {

    SchemaMismatchException() {
    }

    public SchemaMismatchException(String message) {
        super(message);
    }

    public SchemaMismatchException(Throwable cause) {
        super(cause);
    }

    public SchemaMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}

