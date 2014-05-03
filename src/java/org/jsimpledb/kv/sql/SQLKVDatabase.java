
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;

import javax.sql.DataSource;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVDatabaseException;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;

/**
 * Support superclass for SQL {@link KVDatabase} implementations.
 */
public class SQLKVDatabase implements KVDatabase {

    /**
     * Default table name ({@value #DEFAULT_TABLE_NAME}).
     */
    public static final String DEFAULT_TABLE_NAME = "KV";

    /**
     * Default key column name ({@value #DEFAULT_KEY_COLUMN_NAME}).
     */
    public static final String DEFAULT_KEY_COLUMN_NAME = "kv_key";

    /**
     * Default value column name ({@value #DEFAULT_VALUE_COLUMN_NAME}).
     */
    public static final String DEFAULT_VALUE_COLUMN_NAME = "kv_value";

    protected DataSource dataSource;

    /**
     * The name of the key/value table. Default value is {@value #DEFAULT_TABLE_NAME}.
     */
    protected String tableName = DEFAULT_TABLE_NAME;

    /**
     * The name of the key column. Default value is {@value #DEFAULT_KEY_COLUMN_NAME}.
     */
    protected String keyColumnName = DEFAULT_KEY_COLUMN_NAME;

    /**
     * The name of the value column. Default value is {@value #DEFAULT_VALUE_COLUMN_NAME}.
     */
    protected String valueColumnName = DEFAULT_VALUE_COLUMN_NAME;

    /**
     * Get the {@link DataSource} used with this instance.
     */
    public DataSource getDataSource() {
        return this.dataSource;
    }

    /**
     * Configure the {@link DataSource} used with this instance.
     *
     * <p>
     * Required property.
     * </p>
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Get the name of the table containing keys and values.
     *
     * <p>
     * Default value is {@value #DEFAULT_TABLE_NAME}.
     * </p>
     */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * Set the name of the key/value table.
     *
     * @param tableName the name of the key/value table
     * @throws IllegalArgumentException if {@code table} is null
     */
    public void setTableName(String tableName) {
        if (tableName == null)
            throw new IllegalArgumentException("null tableName");
        this.tableName = tableName;
    }

    /**
     * Get the name of the column containing keys.
     *
     * <p>
     * Default value is {@value #DEFAULT_KEY_COLUMN_NAME}.
     * </p>
     */
    public String getKeyColumnName() {
        return this.keyColumnName;
    }

    /**
     * Configure the name of the column containing keys.
     *
     * @param keyColumnName the name of the key column
     * @throws IllegalArgumentException if {@code keyColumnName} is null
     */
    public void setKeyColumnName(String keyColumnName) {
        if (keyColumnName == null)
            throw new IllegalArgumentException("null keyColumn");
        this.keyColumnName = keyColumnName;
    }

    /**
     * Get the name of the column containing values.
     *
     * <p>
     * Default value is {@value #DEFAULT_VALUE_COLUMN_NAME}.
     * </p>
     */
    public String getValueColumnName() {
        return this.valueColumnName;
    }

    /**
     * Configure the name of the column containing values.
     *
     * @param valueColumnName the name of the value column
     * @throws IllegalArgumentException if {@code valueColumnName} is null
     */
    public void setValueColumnName(String valueColumnName) {
        if (valueColumnName == null)
            throw new IllegalArgumentException("null valueColumn");
        this.valueColumnName = valueColumnName;
    }

    /**
     * Create a new transaction.
     *
     * @throws KVDatabaseException if an unexpected error occurs
     * @throws IllegalStateException if no {@link DataSource} is {@linkplain #setDataSource configured}
     */
    @Override
    public SQLKVTransaction createTransaction() {
        if (this.dataSource == null)
            throw new IllegalStateException("no DataSource configured");
        try {
            return this.createSQLKVTransaction(this.createConnection());
        } catch (SQLException e) {
            throw new KVDatabaseException(this, e);
        }
    }

    /**
     * Create a {@link Connection} for a new transaction.
     *
     * <p>
     * Note: this method is responsible for beginning the SQL transaction.
     * </p>
     *
     * <p>
     * The implementation in {@link SQLKVDatabase} invokes {@link DataSource#getConnection()} on the
     * {@linkplain #dataSource configured DataSource} and then {@link Connection#setAutoCommit Connection.setAutoCommit(false)}
     * to open the transaction.
     * </p>
     */
    protected Connection createConnection() throws SQLException {
        final Connection connection = this.dataSource.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * Create a new {@link SQLKVTransaction} for a new transaction given the specified {@link Connection}.
     *
     * <p>
     * The implementation in {@link SQLKVDatabase} just invokes
     * {@link SQLKVTransaction#SQLKVTransaction new SQLKVTransaction(this, connection)}.
     * </p>
     */
    protected SQLKVTransaction createSQLKVTransaction(Connection connection) throws SQLException {
        return new SQLKVTransaction(this, connection);
    }

    /**
     * Create an SQL statement that reads the value column associated with key <code>&#63;1<?code>.
     */
    public String createGetStatement() {
        return "SELECT " + this.quote(this.valueColumnName) + " FROM "
          + this.quote(this.tableName) + " WHERE " + this.quote(this.keyColumnName) + " = ?";
    }

    /**
     * Create an SQL statement that reads the key and value columns (in that order) associated
     * with the smallest key greater than or equal to <code>&#63;1<?code>, if any.
     *
     * @param reverse true to return rows in descending key order, false to return rows in ascending key order
     */
    public String createGetAtLeastStatement(boolean reverse) {
        return "SELECT " + this.quote(this.keyColumnName) + ", " + this.quote(this.valueColumnName)
          + " FROM " + this.quote(this.tableName) + " WHERE " + this.quote(this.keyColumnName)
          + " >= ? ORDER BY " + this.quote(this.keyColumnName) + (reverse ? " DESC" : " ASC");
    }

    /**
     * Create an SQL statement that reads the key and value columns (in that order)
     * associated with the greatest key strictly less than <code>&#63;1<?code>, if any.
     *
     * @param reverse true to return rows in descending key order, false to return rows in ascending key order
     */
    public String createGetAtMostStatement(boolean reverse) {
        return "SELECT " + this.quote(this.keyColumnName) + ", " + this.quote(this.valueColumnName)
          + " FROM " + this.quote(this.tableName) + " WHERE " + this.quote(this.keyColumnName)
          + " < ? ORDER BY " + this.quote(this.keyColumnName) + (reverse ? " DESC" : " ASC");
    }

    /**
     * Create an SQL statement that reads the key and value columns (in that order) associated with all keys
     * in the range <code>&#63;1<?code> (inclusive) to <code>&#63;2<?code> (exclusive), possibly reversed.
     *
     * @param reverse true to return rows in descending key order, false to return rows in ascending key order
     */
    public String createGetRangeStatement(boolean reverse) {
        return "SELECT " + this.quote(this.keyColumnName) + ", " + this.quote(this.valueColumnName)
          + " FROM " + this.quote(this.tableName) + " WHERE " + this.quote(this.keyColumnName)
          + " >= ? and " + this.quote(this.keyColumnName) + " < ? ORDER BY " + this.quote(this.keyColumnName)
          + (reverse ? " DESC" : " ASC");
    }

    /**
     * Create an SQL statement that reads all of the key and value columns (in that order), possibly reversed.
     *
     * @param reverse true to return rows in descending key order, false to return rows in ascending key order
     */
    public String createGetAllStatement(boolean reverse) {
        return "SELECT " + this.quote(this.keyColumnName) + ", " + this.quote(this.valueColumnName)
          + " FROM " + this.quote(this.tableName) + " ORDER BY " + this.quote(this.keyColumnName) + (reverse ? " DESC" : " ASC");
    }

    /**
     * Create an SQL statement that inserts the key/value pair with key <code>&#63;1<?code> and value <code>&#63;2<?code>
     * A row with key <code>&#63;1<?code> may already exist; if so, the value should be updated to <code>&#63;3<?code>.
     */
    public String createPutStatement() {
        return "INSERT INTO " + this.quote(this.tableName) + " (" + this.quote(this.keyColumnName)
          + ", " + this.quote(this.valueColumnName) + ") VALUES (?, ?) ON DUPLICATE KEY UPDATE "
          + this.quote(this.valueColumnName) + " = ?";
    }

    /**
     * Create an SQL statement that deletes the row associated with key <code>&#63;1<?code>, if any.
     * Note that the key may or may not exist prior to this method being invoked.
     */
    public String createRemoveStatement() {
        return "DELETE FROM " + this.quote(this.tableName) + " WHERE " + this.quote(this.keyColumnName) + " = ?";
    }

    /**
     * Create an SQL statement that deletes all rows with keys in the range <code>&#63;1<?code> (inclusive}
     * to <code>&#63;2<?code> (exclusive).
     */
    public String createRemoveRangeStatement() {
        return "DELETE FROM " + this.quote(this.tableName)
          + " WHERE " + this.quote(this.keyColumnName) + " >= ? AND " + this.quote(this.keyColumnName) + " < ?";
    }

    /**
     * Create an SQL statement that deletes all rows with keys greater than or equal to <code>&#63;1<?code>.
     */
    public String createRemoveAtLeastStatement() {
        return "DELETE FROM " + this.quote(this.tableName) + " WHERE " + this.quote(this.keyColumnName) + " >= ?";
    }

    /**
     * Create an SQL statement that deletes all rows with keys strictly less than <code>&#63;1<?code>.
     */
    public String createRemoveAtMostStatement() {
        return "DELETE FROM " + this.quote(this.tableName) + " WHERE " + this.quote(this.keyColumnName) + " < ?";
    }

    /**
     * Create an SQL statement that deletes all rows.
     */
    public String createRemoveAllStatement() {
        return "DELETE FROM " + this.quote(this.tableName);
    }

    /**
     * Modify the given SQL statement so that only one row is returned.
     *
     * <p>
     * This is an optional method; returning {@code statement} unmodified is acceptable, but subclasses may
     * be able to improve efficiency by modifying the SQL statement in a vendor-specific manner to only return one row.
     * </p>
     *
     * <p>
     * The implementation in {@link SQLKVDatabase} returns its parameter unchanged.
     * </p>
     *
     * @param sql SQL statement
     * @return SQL statement
     */
    public String limitSingleRow(String sql) {
        return sql;
    }

    /**
     * Enquote a table or column name as necessary.
     *
     * <p>
     * The implementation in {@link SQLKVDatabase} returns its parameter unchanged.
     * </p>
     *
     * @param name table or column name
     * @return {@code name} enquoted as necessary for this database type
     * @throws IllegalArgumentException if {@code name} is null
     */
    public String quote(String name) {
        return name;
    }

    /**
     * Wrap the given {@link SQLException} in the appropriate {@link KVTransactionException}.
     *
     * @param transaction the {@link SQLKVTransaction} in which the exception occured
     * @param e SQL exception
     * @return appropriate {@link KVTransactionException} with chained exception {@code e}
     * @throws NullPointerException if {@code e} is null
     */
    public KVTransactionException wrapException(SQLKVTransaction transaction, SQLException e) {
        if (e instanceof SQLTimeoutException)
            return new TransactionTimeoutException(transaction, e);
        if (e instanceof SQLRecoverableException)
            return new RetryTransactionException(transaction, e);
        if (e instanceof SQLTransientException)
            return new RetryTransactionException(transaction, e);
        return new KVTransactionException(transaction, e);
    }
}

