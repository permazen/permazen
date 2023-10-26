
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown by {@link Database#createTransaction} when the provided schema is invalid.
 */
@SuppressWarnings("serial")
public class InvalidSchemaException extends DatabaseException {

    InvalidSchemaException() {
    }

    public InvalidSchemaException(String message) {
        super(message);
    }

    public InvalidSchemaException(Throwable cause) {
        super(cause);
    }

    public InvalidSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
