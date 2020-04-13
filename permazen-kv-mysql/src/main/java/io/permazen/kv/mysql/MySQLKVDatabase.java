
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mysql;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;

import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryTransactionException;
import io.permazen.kv.sql.SQLKVDatabase;
import io.permazen.kv.sql.SQLKVTransaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL variant of {@link SQLKVDatabase}.
 *
 * <p>
 * Automatically creates the key/value table on startup if it doesn't already exist.
 */
public class MySQLKVDatabase extends SQLKVDatabase {

    public static final int INNODB_NORMAL_INDEX_SIZE = 767;
    public static final int INNODB_LARGE_INDEX_SIZE = 3072;

    private static final int DEFAULT_LOCK_TIMEOUT = 3;              // 3 seconds

    private boolean innodbLargePrefix;
    private int lockTimeout = DEFAULT_LOCK_TIMEOUT;

    /**
     * Configure the use of InnoDB large prefixes.
     *
     * <p>
     * MySQL InnoDB indexes are normally limited to {@value #INNODB_NORMAL_INDEX_SIZE} bytes, but you can
     * go up to {@value #INNODB_LARGE_INDEX_SIZE} bytes if you set these parameters in {@code /etc/my.cnf}:
     * <pre>
     *  [mysqld]
     *  innodb_large_prefix      = true
     *  innodb_file_per_table    = true
     *  innodb_file_format       = barracuda
     * </pre>
     *
     * <p>
     * Setting this property to true increases the maximum key length from {@value #INNODB_NORMAL_INDEX_SIZE}
     * to {@value #INNODB_LARGE_INDEX_SIZE} bytes and adds {@code ROW_FORMAT=DYNAMIC} to the table creation
     * statement in {@link #initializeDatabaseIfNecessary initializeDatabaseIfNecessary()}. This property has
     * no effect if the table already exists at startup.
     *
     * <p>
     * Default is false.
     *
     * @param innodbLargePrefix true for InnoDB large prefixes
     * @see <a href="http://dev.mysql.com/doc/refman/5.5/en/innodb-restrictions.html">Limits on InnoDB Tables</a>
     */
    public void setInnodbLargePrefix(boolean innodbLargePrefix) {
        this.innodbLargePrefix = innodbLargePrefix;
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
        final int indexSize = this.innodbLargePrefix ? INNODB_LARGE_INDEX_SIZE : INNODB_NORMAL_INDEX_SIZE;
        final String rowFormat = this.innodbLargePrefix ? " ROW_FORMAT=DYNAMIC" : "";
        final String sql = "CREATE TABLE IF NOT EXISTS " + this.quote(this.getTableName()) + " (\n"
          + "  " + this.quote(this.getKeyColumnName()) + " VARBINARY(" + indexSize + ") NOT NULL,\n"
          + "  " + this.quote(this.getValueColumnName()) + " LONGBLOB NOT NULL,\n"
          + "  PRIMARY KEY(" + this.quote(this.getKeyColumnName()) + ")\n"
          + ") ENGINE=InnoDB default charset=utf8 collate=utf8_bin" + rowFormat;
        try (final Statement statement = connection.createStatement()) {
            this.log.debug("auto-creating table `" + this.getTableName() + "' if not already existing:\n{}", sql);
            statement.execute(sql);
        }
    }

    @Override
    protected void configureConnection(Connection connection) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            statement.execute("SET innodb_lock_wait_timeout = " + this.lockTimeout);
            statement.execute("SET SESSION sql_mode = 'TRADITIONAL'");              // force error if key or value is too long
        }
    }

    @Override
    protected SQLKVTransaction createSQLKVTransaction(Connection connection) throws SQLException {
        return new MySQLKVTransaction(this, connection);
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

