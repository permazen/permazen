
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * {@link SQLKVDatabase} transaction.
 */
class MySQLKVTransaction extends SQLKVTransaction {

    MySQLKVTransaction(SQLKVDatabase database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public void setTimeout(long timeout) {
        super.setTimeout(timeout);
        try {
            final Statement statement = this.connection.createStatement();
            statement.execute("SET innodb_lock_wait_timeout = " + (timeout + 999) / 1000);
            statement.close();
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }
}

