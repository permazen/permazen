
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sql;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.jsimpledb.kv.KVDatabaseException;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteOpenMode;

/**
 * SQLite variant of {@link SQLKVDatabase}.
 *
 * <p>
 * Instances need only be configured with the database file, via {@link #setDatabaseFile setDatabaseFile()}.
 * In this case, the {@link SQLiteDataSource} will then be automatically created and configured. The
 * configuration can be overridden via {@link #setSQLiteConfig setSQLiteConfig()} if desired.
 *
 * <p>
 * Otherwise (i.e., if {@link #setDatabaseFile setDatabaseFile()} is not used), then {@link #setDataSource setDataSource()}
 * must be used to explicitly configure a {@link DataSource} and any invocation of {@link #setSQLiteConfig setSQLiteConfig()}
 * is ignored.
 */
public class SQLiteKVDatabase extends SQLKVDatabase {

    /**
     * The special {@link File} that, when configured via {@link #setDatabaseFile setDatabaseFile()},
     * causes SQLite to use an in-memory database.
     */
    public static final File MEMORY_FILE = new File(":memory");

    private static final int DEFAULT_LOCK_TIMEOUT = 10;             // 10 seconds

    private File file;
    private SQLiteConfig config;

    /**
     * Constructor.
     */
    public SQLiteKVDatabase() {
        this.config = new SQLiteConfig();
        this.config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        this.config.setOpenMode(SQLiteOpenMode.READWRITE);
        this.config.setOpenMode(SQLiteOpenMode.CREATE);
        this.config.setOpenMode(SQLiteOpenMode.OPEN_URI);
    }

    /**
     * Get the configured database file.
     *
     * @return configured database file, if any, otherwise null
     */
    public File getDatabaseFile() {
        return this.file;
    }

    /**
     * Set the configured database file.
     *
     * @param file database file
     */
    public void setDatabaseFile(File file) {
        this.file = file;
    }

    /**
     * Set the {@link SQLiteConfig} used to configure the auto-created {@link SQLiteDataSource}
     * when a database file is {@linkplain #setDatabaseFile specified} but no
     *
     * @param config connection config, or null for none
     */
    public void setSQLiteConfig(SQLiteConfig config) {
        this.config = config;
    }

// Overrides

    @Override
    public void start() {
        super.start();

        // Auto-configure DataSource
        if (this.getDataSource() == null && this.file != null) {
            final SQLiteDataSource dataSource = this.config != null ? new SQLiteDataSource(this.config) : new SQLiteDataSource();
            final String uri = "jdbc:sqlite:" + this.file.toURI().toString().substring("file:".length());
            this.log.debug("auto-configuring SQLite DataSource using URI `" + uri + "'");
            dataSource.setUrl(uri);
            this.setDataSource(dataSource);
        }

        // Auto-create table
        final String sql = "CREATE TABLE IF NOT EXISTS " + this.quote(this.getTableName()) + "(\n"
          + "  " + this.quote(this.getKeyColumnName()) + " BLOB\n"
            + "    CONSTRAINT " + this.quote(this.getKeyColumnName() + "_null") + " NOT NULL\n"
            + "    CONSTRAINT " + this.quote(this.getKeyColumnName() + "_pkey") + " PRIMARY KEY,\n"
          + "  " + this.quote(this.getValueColumnName()) + " BLOB\n"
            + "    CONSTRAINT " + this.quote(this.getValueColumnName() + "_null") + " NOT NULL\n"
          + ")";
        try (final Connection connection = this.createTransactionConnection()) {
            this.beginTransaction(connection);
            try (final Statement statement = connection.createStatement()) {
                this.log.debug("auto-creating table `" + this.getTableName() + "' if not already existing:\n{}", sql);
                statement.execute(sql);
                statement.execute("COMMIT");
            }
        } catch (SQLException e) {
            throw new KVDatabaseException(this, e);
        }
        this.log.debug("SQLite database " + (this.file != null ? this.file + " " : "") + "started");
    }

    @Override
    public String createPutStatement() {
        return "INSERT OR REPLACE INTO " + this.quote(this.getTableName()) + " (" + this.quote(this.getKeyColumnName())
          + ", " + this.quote(this.getValueColumnName()) + ") VALUES (?, ?)";
    }

    /**
     * Encloses the given {@code name} in double quotes.
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
        switch (SQLiteErrorCode.getErrorCode(e.getErrorCode())) {
        case SQLITE_BUSY:
        case SQLITE_LOCKED:
            return new RetryTransactionException(tx, e);
        default:
            return super.wrapException(tx, e);
        }
    }
}

