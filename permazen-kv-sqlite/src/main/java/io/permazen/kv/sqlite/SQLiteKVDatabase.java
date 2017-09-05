
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.sqlite;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryTransactionException;
import io.permazen.kv.sql.SQLKVDatabase;
import io.permazen.kv.sql.SQLKVTransaction;
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
 * must be used to explicitly configure a {@link javax.sql.DataSource} and any invocation of
 * {@link #setSQLiteConfig setSQLiteConfig()} is ignored.
 */
public class SQLiteKVDatabase extends SQLKVDatabase {

    /**
     * The name of the SQLite JDBC driver class ({@value #SQLITE_DRIVER_CLASS_NAME}).
     */
    public static final String SQLITE_DRIVER_CLASS_NAME = "org.sqlite.JDBC";

    /**
     * The special {@link File} that, when configured via {@link #setDatabaseFile setDatabaseFile()},
     * causes SQLite to use an in-memory database.
     */
    public static final File MEMORY_FILE = new File(":memory");

    private static final int DEFAULT_LOCK_TIMEOUT = 10;             // 10 seconds

    private File file;
    private SQLiteConfig config;
    private boolean exclusiveLocking;
    private List<String> pragmas;

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
     * Set an {@link SQLiteConfig} with which to configure the auto-created {@link SQLiteDataSource}.
     *
     * @param config connection config, or null for none
     */
    public void setSQLiteConfig(SQLiteConfig config) {
        this.config = config;
    }

    /**
     * Configure whether the database connection locking mode uses exclusive locking.
     * This provides a performance benefit, but requires that this Java process be the only one accessing the database.
     *
     * <p>
     * Default is normal (non-exclusive) locking.
     *
     * @param exclusiveLocking true for exclusive locking, false for normal locking
     * @see <a href="https://www.sqlite.org/pragma.html#pragma_locking_mode">PRAGMA schema.locking_mode</a>
     */
    public void setExclusiveLocking(boolean exclusiveLocking) {
        this.exclusiveLocking = exclusiveLocking;
    }

    /**
     * Configure arbitrary {@code PRAGMA} statements to be executed on newly created {@link Connection}s.
     *
     * @param pragmas zero or more pragma statements (without the {@code PRAGMA} keyword) to execute on new {@link Connection}s
     * @see <a href="https://www.sqlite.org/pragma.html">PRAGMA statements</a>
     */
    public void setPragmas(List<String> pragmas) {
        this.pragmas = pragmas;
    }

// Overrides

    @Override
    public void start() {

        // Ensure driver class is loaded
        try {
            Class.forName(SQLITE_DRIVER_CLASS_NAME, false, Thread.currentThread().getContextClassLoader());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("can't load SQLite driver class `" + SQLITE_DRIVER_CLASS_NAME + "'", e);
        }

        // Auto-configure DataSource
        if (this.getDataSource() == null && this.file != null) {
            final SQLiteDataSource dataSource = this.config != null ? new SQLiteDataSource(this.config) : new SQLiteDataSource();
            final String uri = "jdbc:sqlite:" + this.file.toURI().toString().substring("file:".length());
            this.log.debug("auto-configuring SQLite DataSource using URI `" + uri + "'");
            dataSource.setUrl(uri);
            this.setDataSource(dataSource);
        }

        // Proceed
        super.start();
        this.log.debug("SQLite database " + (this.file != null ? this.file + " " : "") + "started");
    }

    @Override
    protected void initializeDatabaseIfNecessary(Connection connection) throws SQLException {
        final String sql = "CREATE TABLE IF NOT EXISTS " + this.quote(this.getTableName()) + "(\n"
          + "  " + this.quote(this.getKeyColumnName()) + " BLOB\n"
            + "    CONSTRAINT " + this.quote(this.getKeyColumnName() + "_null") + " NOT NULL\n"
            + "    CONSTRAINT " + this.quote(this.getKeyColumnName() + "_pkey") + " PRIMARY KEY,\n"
          + "  " + this.quote(this.getValueColumnName()) + " BLOB\n"
            + "    CONSTRAINT " + this.quote(this.getValueColumnName() + "_null") + " NOT NULL\n"
          + ")";
        this.beginTransaction(connection);
        try (final Statement statement = connection.createStatement()) {
            this.log.debug("auto-creating table `" + this.getTableName() + "' if not already existing:\n{}", sql);
            statement.execute(sql);
            statement.execute("COMMIT");
        }
        this.log.debug("SQLite database " + (this.file != null ? this.file + " " : "") + "started");
    }

    @Override
    protected void configureConnection(Connection connection) throws SQLException {
        if (!this.exclusiveLocking && (this.pragmas == null || this.pragmas.isEmpty()))
            return;
        try (final Statement statement = connection.createStatement()) {
            if (this.exclusiveLocking) {
                this.log.debug("configuring database connection for exclusive locking");
                statement.execute("PRAGMA locking_mode=EXCLUSIVE");
            }
            if (this.pragmas != null) {
                for (String pragma : this.pragmas) {
                    this.log.debug("configuring database connection with PRAGMA " + pragma);
                    statement.execute("PRAGMA " + pragma);
                }
            }
        }
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
