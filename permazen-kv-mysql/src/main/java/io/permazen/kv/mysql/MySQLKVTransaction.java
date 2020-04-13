
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mysql;

import io.permazen.kv.sql.SQLKVDatabase;
import io.permazen.kv.sql.SQLKVTransaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL variant of {@link SQLKVTransaction}.
 */
class MySQLKVTransaction extends SQLKVTransaction {

    MySQLKVTransaction(SQLKVDatabase database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public void setTimeout(long timeout) {
        super.setTimeout(timeout);
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("SET innodb_lock_wait_timeout = " + (timeout + 999) / 1000);
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }
}

