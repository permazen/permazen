
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.cockroach;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.sql.SQLKVDatabase;
import org.jsimpledb.kv.sql.SQLKVTransaction;

/**
 * {@link org.jsimpledb.kv.KVDatabase} implementation based on CockroachDB.
 *
 * <p>
 * Automatically creates the key/value table on startup if it doesn't already exist.
 *
 * <p>
 * <b>WARNING:</b> not fully working yet; see <a href="https://github.com/cockroachdb/cockroach/issues/1962">issue #1962</a>.
 */
public class CockroachKVDatabase extends SQLKVDatabase {

    @Override
    protected void initializeDatabaseIfNecessary(Connection connection) throws SQLException {
        final String sql = "CREATE TABLE IF NOT EXISTS " + this.quote(this.getTableName()) + " (\n"
          + "  " + this.quote(this.getKeyColumnName()) + " BYTES PRIMARY KEY NOT NULL,\n"
          + "  " + this.quote(this.getValueColumnName()) + " BYTES NOT NULL\n"
          + ")";
        try (final Statement statement = connection.createStatement()) {
            this.log.debug("auto-creating table `" + this.getTableName() + "' if not already existing:\n{}", sql);
            statement.execute(sql);
        }
    }

    @Override
    protected void beginTransaction(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
    }

    @Override
    protected void postBeginTransaction(Connection connection) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            statement.execute("SET TRANSACTION ISOLATION LEVEL " + this.isolationLevel.name().replace('_', ' '));
        }
    }

    @Override
    protected CockroachKVTransaction createSQLKVTransaction(Connection connection) throws SQLException {
        return new CockroachKVTransaction(this, connection);
    }

    @Override
    public String createPutStatement() {
        // XXX Requires this issue to be fixed: https://github.com/cockroachdb/cockroach/issues/1962
        return "REPLACE INTO " + this.quote(this.tableName) + " (" + this.quote(this.keyColumnName)
          + ", " + this.quote(this.valueColumnName) + ") VALUES (?, ?)";
    }

    /**
     * Encloses the given {@code name} in backticks.
     */
    @Override
    public String quote(String name) {
        return "\"" + name + "\"";
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
        final String state = e.getSQLState();
        switch (state) {
        case "55P03":                                   // lock not available
        case "40P01":                                   // deadlock detected
        case "CR000":                                   // See https://groups.google.com/forum/#!topic/cockroach-db/FpBemFJM4w8
            return new RetryTransactionException(tx, e);
        default:
            return super.wrapException(tx, e);
        }
    }
}
