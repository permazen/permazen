
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mssql;

import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryTransactionException;
import io.permazen.kv.sql.SQLKVDatabase;
import io.permazen.kv.sql.SQLKVTransaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Microsoft SQL Server variant of {@link SQLKVDatabase}.
 *
 * <p>
 * Automatically creates the key/value table on startup if it doesn't already exist.
 *
 * <p>
 * Note: Because <a href="http://stackoverflow.com/questions/26255195/how-does-varbinary-data-sorted/26257328">SQL Server
 * ignores trailing zero bytes when comparing {@code VARBINARY} values</a>, we are forced to use "byte stuffing"
 * to encode keys, whereby {@code 0x00} and {@code 0x01} are replaced by {@code 0x0101} and {@code 0x0102}, respectively.
 */
public class MSSQLKVDatabase extends SQLKVDatabase {

    public static final String DEFAULT_SCHEMA = "dbo";
    public static final boolean DEFAULT_LARGE_KEYS = false;
    public static final boolean DEFAULT_LARGE_VALUES = true;
    public static final int DEFAULT_LOCK_TIMEOUT = 3000;

    private static final int MAX_SMALL_VARBINARY = 8000;

    private boolean largeKeys = DEFAULT_LARGE_KEYS;
    private boolean largeValues = DEFAULT_LARGE_VALUES;

    private int lockTimeout = DEFAULT_LOCK_TIMEOUT;
    private String schema = DEFAULT_SCHEMA;

    /**
     * Get schema name.
     *
     * <p>
     * Default is {@value #DEFAULT_SCHEMA}.
     *
     * @return schema name
     */
    public String getSchema() {
        return this.schema;
    }
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Get whether support for large keys (greater than 8,000 bytes) are enabled.
     *
     * <p>
     * This only affects initial table creation; it will have no effect on an already-initialized database.
     *
     * <p>
     * Note that if {@linkplain #isLargeValues large values} are enabled, and any object field containing values
     * greater than 8,000 bytes is indexed, then large keys must also be enabled.
     *
     * <p>
     * Default is {@value #DEFAULT_LARGE_KEYS}.
     *
     * @return true if large key support is enabled
     */
    public boolean isLargeKeys() {
        return this.largeKeys;
    }
    public void setLargeKeys(boolean largeKeys) {
        this.largeKeys = largeKeys;
    }

    /**
     * Get whether support for large values (greater than 8,000 bytes) are enabled.
     *
     * <p>
     * This only affects initial table creation; it will have no effect on an already-initialized database.
     *
     * <p>
     * Note that if large values are enabled, and any object field containing values
     * greater than 8,000 bytes is indexed, then {@linkplain #isLargeKeys large keys} must also be enabled.
     *
     *
     * <p>
     * Default is {@value #DEFAULT_LARGE_VALUES}.
     *
     * @return true if large value support is enabled
     */
    public boolean isLargeValues() {
        return this.largeValues;
    }
    public void setLargeValues(boolean largeValues) {
        this.largeValues = largeValues;
    }

    /**
     * Get the lock timeout with which to configure new transactions.
     *
     * <p>
     * Default is {@value #DEFAULT_LOCK_TIMEOUT}.
     *
     * @return lock timeout for new transactions
     */
    public int getLockTimeout() {
        return this.lockTimeout;
    }
    public void setLockTimeout(int lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    @Override
    protected void initializeDatabaseIfNecessary(Connection connection) throws SQLException {
        final String keySize = this.largeKeys ? "max" : "" + MAX_SMALL_VARBINARY;
        final String valSize = this.largeValues ? "max" : "" + MAX_SMALL_VARBINARY;
        final String sql = ""
          + "if not exists (select * from sys.tables t\n"
          + "   join sys.schemas s on (t.schema_id = s.schema_id)\n"
          + "   where s.name = " + this.quoteString(this.schema) + "\n"
          + "     and t.name = " + this.quoteString(this.getTableName()) + ")\n"
          + "create table " + this.getTableName() + " (\n"
          + "  " + this.getKeyColumnName() + " VARBINARY(" + keySize + ") NOT NULL PRIMARY KEY,\n"
          + "  " + this.getValueColumnName() + " VARBINARY(" + valSize + ") NOT NULL\n"
          + ")\n";
        try (Statement statement = connection.createStatement()) {
            this.log.debug("auto-creating table `{}' if not already existing:\n{}", this.getTableName(), sql);
            statement.execute(sql);
        }
    }

    @Override
    protected void configureConnection(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET LOCK_TIMEOUT " + this.lockTimeout);
        }
    }

    @Override
    protected SQLKVTransaction createSQLKVTransaction(Connection connection) throws SQLException {
        return new MSSQLKVTransaction(this, connection);
    }

    // http://stackoverflow.com/questions/108403/solutions-for-insert-or-update-on-sql-server
    // https://samsaffron.com/blog/archive/2007/04/04/14.aspx
    @Override
    public String createPutStatement() {
        final String tn = this.quote(this.tableName);
        final String kc = this.quote(this.keyColumnName);
        final String vc = this.quote(this.valueColumnName);
        return ""
          + "merge " + tn + " with(HOLDLOCK) as target\n"
          + "  using (values (?))\n"
          + "  as source (" + vc + ")\n"
          + "  on target." + kc + " = ?\n"
          + "when matched then\n"
          + "  update set " + vc + " = source." + vc + "\n"
          + "when not matched then\n"
          + "  insert (" + kc + ", " + vc + ")\n"
          + "  values (?, ?);";

/*
        return ""
          + "BEGIN\n"
          + "  UPDATE " + tn + " WITH (SERIALIZABLE) SET " + vc + " = ?\n"
          + "  WHERE " + kc + " = ?\n"
          + "  IF @@rowcount = 0\n"
          + "  BEGIN\n"
          + "    INSERT " + tn + "\n"
          + "      (" + kc + ", " + vc + ")\n"
          + "    VALUES\n"
          + "      (?, ?)\n"
          + "  END\n"
          + "END\n";
*/
    }

    /**
     * Encloses the given {@code name} in single quotes and doubles up any single quotes therein.
     *
     * @param value string to quote
     * @return quoted string
     */
    protected String quoteString(String value) {
        return "'" + value.replaceAll("'", "''") + "'";
    }

    @Override
    public String limitSingleRow(String sql) {
        return sql.replaceAll("(?i)^SELECT ", "SELECT TOP 1 ");
    }

    @Override
    public KVTransactionException wrapException(SQLKVTransaction tx, SQLException e) {
        switch (e.getErrorCode()) {
        case 1222:                                                      // lock timeout exceeded
        case 1205:                                                      // deadlock
        case 8645:                                                      // server can't allocate memory
            return new RetryTransactionException(tx, e);
        default:
            return super.wrapException(tx, e);
        }
    }
}
