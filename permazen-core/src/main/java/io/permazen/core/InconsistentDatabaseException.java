
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when inconsistent content is detected in a {@link Database} indicating a corrupted or invalid database,
 * or a buggy underlying key-value store.
 *
 * <p>
 * If you get one of these exceptions, you may consider running the {@code jsck} command in the Permazen CLI,
 * which checks for database consistency problems and (optionally) attempts to repair them.
 *
 * @see io.permazen.jsck.Jsck
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
