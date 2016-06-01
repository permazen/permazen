
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sql;

import java.sql.Connection;

/**
 * Standard transaction isolation levels.
 */
public enum IsolationLevel {
    READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

    private final int connectionIsolation;

    IsolationLevel(int connectionIsolation) {
        this.connectionIsolation = connectionIsolation;
    }

    /**
     * Get the value corresponding to this isolation level suitable for {@link Connection#setTransactionIsolation}.
     */
    public int getConnectionIsolation() {
        return this.connectionIsolation;
    }
}

