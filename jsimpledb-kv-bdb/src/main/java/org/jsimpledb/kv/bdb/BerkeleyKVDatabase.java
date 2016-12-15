
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.bdb;

import com.google.common.base.Preconditions;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVDatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oracle Berkeley DB Java Edition {@link KVDatabase} implementation.
 *
 * <p>
 * A {@linkplain #setDirectory database directory} is the only required configuration property, but the
 * {@link Environment}, {@link Database}, and {@link Transaction}s may all be configured.
 * Instances may be stopped and (re)started multiple times.
 *
 * @see <a href="http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/overview/index-093405.html"
 *  >Oracle Berkeley DB Java Edition</a>
 */
public class BerkeleyKVDatabase implements KVDatabase {

// Locking order: (1) BerkeleyKVTransaction, (2) BerkeleyKVDatabase

    /**
     * Default Berkeley DB database name ({@value #DEFAULT_DATABASE_NAME}).
     *
     * @see #setDatabaseName setDatabaseName()
     */
    public static final String DEFAULT_DATABASE_NAME = "JSimpleDB";

    private static final ThreadLocal<TransactionConfig> NEXT_TX_CONFIG = new ThreadLocal<>();

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final DatabaseConfig databaseConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true);
    private final HashSet<BerkeleyKVTransaction> openTransactions = new HashSet<>();
    private EnvironmentConfig environmentConfig = new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setTxnSerializableIsolation(true);
    private TransactionConfig defaultTransactionConfig = TransactionConfig.DEFAULT;
    private File directory;
    private String databaseName = DEFAULT_DATABASE_NAME;

    private Environment environment;
    private Database database;

    /**
     * Constructor.
     */
    public BerkeleyKVDatabase() {
    }

    /**
     * Get the filesystem directory containing the database.
     *
     * @return database directory
     */
    public synchronized File getDirectory() {
        return this.directory;
    }

    /**
     * Configure the filesystem directory containing the database. Required property.
     *
     * @param directory database directory
     */
    public synchronized void setDirectory(File directory) {
        this.directory = directory;
    }

    /**
     * Get the configured {@link Database} name.
     *
     * @return database name
     */
    public synchronized String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Configure the {@link Database} name. Defaults to {@link #DEFAULT_DATABASE_NAME}.
     *
     * @param databaseName database name
     */
    public synchronized void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Get the {@link EnvironmentConfig} to be used by this instance.
     *
     * <p>
     * This method returns a copy; use {@link #setEnvironmentConfig setEnvironmentConfig()} to change.
     *
     * @return environment config
     */
    public synchronized EnvironmentConfig getEnvironmentConfig() {
        return this.environmentConfig.clone();
    }

    /**
     * Set a custom {@link EnvironmentConfig} to be used by this instance at startup.
     *
     * <p>
     * The default {@link EnvironmentConfig} is configured for
     * {@linkplain EnvironmentConfig#getTxnSerializableIsolation serializable}
     * {@linkplain EnvironmentConfig#getTransactional transactional} operation and with
     * {@linkplain EnvironmentConfig#getAllowCreate allowCreate} set to true.
     *
     * <p>
     * The given {@code config} must at least be configured for transactional operation.
     *
     * @param config environment config
     * @throws IllegalArgumentException if {@code config} is not configured to be
     *  {@linkplain EnvironmentConfig#getTransactional transactional}
     * @throws IllegalArgumentException if {@code config} is null
     */
    public synchronized void setEnvironmentConfig(EnvironmentConfig config) {
        Preconditions.checkArgument(config != null, "null config");
        Preconditions.checkArgument(config.getTransactional(), "environment config must be transactional");
        this.environmentConfig = config;
    }

    /**
     * Get the default {@link TransactionConfig} to be used by this instance.
     *
     * <p>
     * This method returns a copy; use {@link #setTransactionConfig setTransactionConfig()} to change.
     *
     * @return transaction config
     */
    public synchronized TransactionConfig getTransactionConfig() {
        return this.defaultTransactionConfig.clone();
    }

    /**
     * Configure a custom default {@link TransactionConfig} to be used by this instance for transactions.
     *
     * <p>
     * Note: this configures the default; this default config can be overridden <i>for the next transaction in the
     * current thread only</i> via {@link #setNextTransactionConfig setNextTransactionConfig()}.
     *
     * <p>
     * The default setting for this property is {@link TransactionConfig#DEFAULT}.
     *
     * @param config transaction config
     * @throws IllegalArgumentException if {@code config} is null
     * @see #setNextTransactionConfig setNextTransactionConfig()
     */
    public synchronized void setTransactionConfig(TransactionConfig config) {
        Preconditions.checkArgument(config != null, "null config");
        this.defaultTransactionConfig = config;
    }

    /**
     * Configure a custom {@link TransactionConfig} to be used for the next transaction in the current thread.
     * The next, and only the next, invocation of {@link #createTransaction} in the current thread will
     * use the given configuration. After that, subsequent transactions will revert back to the
     * {@linkplain #setTransactionConfig default} (even if the invocation of {@link #createTransaction} failed).
     *
     * @param config transaction config
     * @throws IllegalArgumentException if {@code config} is null
     * @see #setTransactionConfig setTransactionConfig()
     */
    public void setNextTransactionConfig(TransactionConfig config) {
        Preconditions.checkArgument(config != null, "null config");
        NEXT_TX_CONFIG.set(config);
    }

    /**
     * Get the {@link DatabaseConfig} to be used by this instance.
     *
     * <p>
     * This method returns a copy; use {@link #setDatabaseConfig setDatabaseConfig()} to change.
     *
     * @return database config
     */
    public synchronized DatabaseConfig getDatabaseConfig() {
        return this.databaseConfig.clone();
    }

    /**
     * Apply selected database configuration parameters from the given instance to the
     * {@link DatabaseConfig} this instance will use when opening the {@link Database} at startup.
     *
     * <p>
     * The default {@link DatabaseConfig} is configured for transactional operation and with
     * {@linkplain EnvironmentConfig#getAllowCreate allowCreate} set to true.
     *
     * Only certain allowed configuration properties are copied. The copied properties are:
     * <ul>
     * <li>{@linkplain DatabaseConfig#getAllowCreate allowCreate}</li>
     * <li>{@linkplain DatabaseConfig#getCacheMode cacheMode}</li>
     * <li>{@linkplain DatabaseConfig#getDeferredWrite deferredWrite}</li>
     * <li>{@linkplain DatabaseConfig#getExclusiveCreate exclusiveCreate}</li>
     * <li>{@linkplain DatabaseConfig#getNodeMaxEntries nodeMaxEntries}</li>
     * <li>{@linkplain DatabaseConfig#getReadOnly readOnly}</li>
     * <li>{@linkplain DatabaseConfig#getReplicated replicated}</li>
     * <li>{@linkplain DatabaseConfig#getTemporary temporary}</li>
     * </ul>
     *
     * @param config database config
     */
    public synchronized void setDatabaseConfig(DatabaseConfig config) {
        this.databaseConfig
          .setAllowCreate(config.getAllowCreate())
          .setCacheMode(config.getCacheMode())
          .setDeferredWrite(config.getDeferredWrite())
          .setExclusiveCreate(config.getExclusiveCreate())
          .setNodeMaxEntries(config.getNodeMaxEntries())
          .setReadOnly(config.getReadOnly())
          .setReplicated(config.getReplicated())
          .setTemporary(config.getTemporary());
    }

    /**
     * Get the underlying {@link Environment} associated with this instance.
     *
     * @return the associated {@link Environment}
     * @throws IllegalStateException if this instance is not {@linkplain #start started}
     */
    public synchronized Environment getEnvironment() {
        Preconditions.checkState(this.environment != null, "not started");
        assert this.database != null;
        return this.environment;
    }

    /**
     * Get the underlying {@link Database} associated with this instance.
     *
     * @return the associated {@link Database}
     * @throws IllegalStateException if this instance is not {@linkplain #start started}
     */
    public synchronized Database getDatabase() {
        Preconditions.checkState(this.environment != null, "not started");
        assert this.database != null;
        return this.database;
    }

// KVDatabase

    @Override
    public BerkeleyKVTransaction createTransaction(Map<String, ?> options) {
        return this.createTransaction();                                            // no options supported yet
    }

    @Override
    public synchronized BerkeleyKVTransaction createTransaction() {

        // Check open
        Preconditions.checkState(this.environment != null, "not started");
        assert this.database != null;

        // Get the config for this transaction
        TransactionConfig config = NEXT_TX_CONFIG.get();
        if (config == null)
            config = this.defaultTransactionConfig;
        else
            NEXT_TX_CONFIG.remove();

        // Create the transaction
        final Transaction bdbTx;
        try {
            bdbTx = this.environment.beginTransaction(null, config);
        } catch (DatabaseException e) {
            throw new KVDatabaseException(this, e);
        }
        final BerkeleyKVTransaction tx = new BerkeleyKVTransaction(this, bdbTx);

        // Record transaction for possible cleanup on shutdown
        this.openTransactions.add(tx);

        // Done
        return tx;
    }

    /**
     * Remove a transaction that is now closed.
     */
    synchronized void removeTransaction(BerkeleyKVTransaction tx) {
        this.openTransactions.remove(tx);
    }

    @Override
    @PostConstruct
    public synchronized void start() {
        assert (this.environment == null) == (this.database == null);
        if (this.environment != null)                                                   // already started
            return;
        assert this.openTransactions.isEmpty();
        Preconditions.checkState(this.directory != null, "no directory configured");
        Preconditions.checkState(this.databaseName != null, "no database name configured");
        boolean success = false;
        try {
            this.environment = new Environment(this.directory, this.environmentConfig);
            this.database = this.environment.openDatabase(null, this.databaseName, this.databaseConfig);
            success = true;
        } finally {
            if (!success) {
                try {
                    for (AutoCloseable item : new AutoCloseable[] { this.database, this.environment }) {
                        if (item != null) {
                            try {
                                item.close();
                            } catch (Throwable e) {
                                this.log.error("caught exception cleaning up after failed startup (ignoring)", e);
                            }
                        }
                    }
                } finally {
                    this.database = null;
                    this.environment = null;
                }
            }
        }
    }

    @Override
    @PreDestroy
    public void stop() {

        // Mark this instance as shutdown so no new transactions are created
        final Environment oldEnvironment;
        final Database oldDatabase;
        final ArrayList<BerkeleyKVTransaction> oldTransactions;
        synchronized (this) {
            assert (this.environment == null) == (this.database == null);
            oldEnvironment = this.environment;
            oldDatabase = this.database;
            oldTransactions = new ArrayList<>(this.openTransactions);
            this.environment = null;
            this.database = null;
            this.openTransactions.clear();
        }

        // Were we already stopped?
        if (oldEnvironment == null)
            return;

        // Rollback any open transactions so cursors are cleaned up and transactions closed
        for (BerkeleyKVTransaction tx : oldTransactions) {
            try {
                tx.rollback();
            } catch (Throwable e) {
                this.log.debug("caught exception closing open transaction during shutdown (ignoring)", e);
            }
        }

        // Shut down database
        try {
            oldDatabase.close();
        } catch (Throwable e) {
            this.log.error("caught exception closing database during shutdown (ignoring)", e);
        }

        // Shut down environment
        try {
            oldEnvironment.close();
        } catch (Throwable e) {
            this.log.error("caught exception closing environment during shutdown (ignoring)", e);
        }
    }
}

