
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sql;

import com.mysql.jdbc.MysqlErrorNumbers;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;

/**
 * MySQL variant of {@link SQLKVDatabase}.
 *
 * <p>
 * An idempotent script for initializing a MySQL database is available at
 * {@code classpath:org/jsimpledb/kv/sql/createTable-mysql.sql}
 * (see also {@link org.dellroad.stuff.schema.UpdatingDataSource}).
 * </p>
 *
 */
public class MySQLKVDatabase extends SQLKVDatabase {

    private static final int DEFAULT_LOCK_TIMEOUT = 10;             // 10 seconds

    @Override
    protected void preBeginTransaction(Connection connection) throws SQLException {
        final Statement statement = connection.createStatement();
        statement.execute("SET TRANSACTION ISOLATION LEVEL " + this.isolationLevel.name().replace('_', ' '));
        statement.execute("SET innodb_lock_wait_timeout = " + DEFAULT_LOCK_TIMEOUT);
        statement.execute("SET SESSION sql_mode = 'TRADITIONAL'");              // force error if key or value is too long
        statement.close();
    }

    /**
     * Encloses the given {@code name} in backticks.
     */
    @Override
    public String quote(String name) {
        return "`" + name + "`";
    }

    /**
     * Appends {@code LIMIT 1} to the statement.
     */
    @Override
    public String limitSingleRow(String sql) {
        return sql + " LIMIT 1";
    }

    @Override
    public KVTransactionException wrapException(SQLKVTransaction tx, SQLException e) {
        switch (e.getErrorCode()) {
        case MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT:
            return new RetryTransactionException(tx, e);
        case MysqlErrorNumbers.ER_LOCK_DEADLOCK:
            return new RetryTransactionException(tx, e);
        default:
            if (e instanceof MySQLTimeoutException)
                return new RetryTransactionException(tx, e);
            return super.wrapException(tx, e);
        }
    }
}

