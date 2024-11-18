
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.cockroach;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.sql.SQLKVDatabase;
import io.permazen.kv.sql.SQLKVTransaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * {@link KVDatabase} implementation based on CockroachDB.
 *
 * <p>
 * Automatically creates the key/value table on startup if it doesn't already exist.
 */
public class CockroachKVDatabase extends SQLKVDatabase {

    @SuppressWarnings("this-escape")
    public CockroachKVDatabase() {
        // https://forum.cockroachlabs.com/t/read-only-transactions-consistency-and-rollback/452
        this.setRollbackForReadOnly(false);
    }

    @Override
    protected void initializeDatabaseIfNecessary(Connection connection) throws SQLException {
        final String sql = "CREATE TABLE IF NOT EXISTS " + this.quote(this.getTableName()) + " (\n"
          + "  " + this.quote(this.getKeyColumnName()) + " BYTES PRIMARY KEY NOT NULL,\n"
          + "  " + this.quote(this.getValueColumnName()) + " BYTES NOT NULL\n"
          + ")";
        try (Statement statement = connection.createStatement()) {
            this.log.debug("auto-creating table \"{}\" if not already existing:\n{}", this.getTableName(), sql);
            statement.execute(sql);
        }
    }

    @Override
    protected void beginTransaction(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
    }

    @Override
    protected void postBeginTransaction(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET TRANSACTION ISOLATION LEVEL " + this.isolationLevel.name().replace('_', ' '));
        }
    }

    @Override
    protected CockroachKVTransaction createSQLKVTransaction(Connection connection) throws SQLException {
        return new CockroachKVTransaction(this, connection);
    }

    @Override
    public String createPutStatement() {
        return "UPSERT INTO " + this.quote(this.tableName) + " (" + this.quote(this.keyColumnName)
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
            return new RetryKVTransactionException(tx, e);
        default:
            if (e.getMessage().contains("restart transaction"))
                return new RetryKVTransactionException(tx, e);
            return super.wrapException(tx, e);
        }
    }
}
