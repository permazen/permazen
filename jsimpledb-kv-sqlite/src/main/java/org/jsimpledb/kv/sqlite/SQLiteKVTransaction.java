
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sqlite;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.jsimpledb.kv.sql.SQLKVDatabase;
import org.jsimpledb.kv.sql.SQLKVTransaction;

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
