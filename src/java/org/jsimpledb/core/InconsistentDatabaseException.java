
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

/**
 * Thrown when inconsistent content is detected in a {@link Database} indicating a corrupted or invalid database,
 * or a buggy underlying key-value store.
 */
@SuppressWarnings("serial")
public class InconsistentDatabaseException extends DatabaseException {

    InconsistentDatabaseException() {
    }

    public InconsistentDatabaseException(String message) {
        super(message);
    }

    public InconsistentDatabaseException(Throwable cause) {
        super(cause);
    }

    public InconsistentDatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}

