
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * SQLite variant of {@link SQLKVTransaction}.
 */
class SQLiteKVTransaction extends SQLKVTransaction {

    SQLiteKVTransaction(SQLKVDatabase database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public void setTimeout(long timeout) {
        super.setTimeout(timeout);
        try (final Statement statement = this.connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout=" + timeout);
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }
}

