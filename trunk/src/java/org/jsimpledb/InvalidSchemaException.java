
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Thrown by {@link JSimpleDB#createTransaction} when the given schema is invalid, cannot be matched with the
 * schema of the same version already recorded in the database, or cannot be added to the database.
 */
@SuppressWarnings("serial")
public class InvalidSchemaException extends JSimpleDBException {

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

