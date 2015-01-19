
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.bdb;

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
 * </p>
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
    private volatile TransactionConfig defaultTransactionConfig = TransactionConfig.DEFAULT;
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
     * Configure the filesystem directory containing the database. Required property.
     */
    public synchronized void setDirectory(File directory) {
        this.directory = directory;
    }

    /**
     * Configure the database name. Defaults to {@link #DEFAULT_DATABASE_NAME}.
     */
    public synchronized void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Set a custom {@link EnvironmentConfig} to be used by this instance.
     *
     * <p>
     * The default {@link EnvironmentConfig} is configured for
     * {@linkplain EnvironmentConfig#getTxnSerializableIsolation serializable}
     * {@linkplain EnvironmentConfig#getTransactional transactional} operation and with
     * {@linkplain EnvironmentConfig#getAllowCreate allowCreate} set to true.
     * </p>
     *
     * <p>
     * The given {@code config} must at least be configured for transactional operation.
     * </p>
     *
     * @param config environment config
     * @throws IllegalArgumentException if {@code config} does not configured to be
     *  {@linkplain EnvironmentConfig#getTransactional transactional}
     */
    public synchronized void setEnvironmentConfig(EnvironmentConfig config) {
        if (!config.getTransactional())
            throw new IllegalArgumentException("environment config must be transactional");
        this.environmentConfig = config;
    }

    /**
     * Configure a custom default {@link TransactionConfig} to be used by this instance for transactions.
     * This configures the default, which can be overridden <i>for the next transaction in the
     * current thread only</i> via {@link #setNextTransactionConfig setNextTransactionConfig()}.
     *
     * <p>
     * The default setting for this property is {@link TransactionConfig#DEFAULT}.
     * </p>
     *
     * @param config transaction config
     * @throws IllegalArgumentException if {@code config} is null
     * @see #setNextTransactionConfig setNextTransactionConfig()
     */
    public void setTransactionConfig(TransactionConfig config) {
        if (config == null)
            throw new IllegalArgumentException("null config");
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
        if (config == null)
            throw new IllegalArgumentException("null config");
        NEXT_TX_CONFIG.set(config);
    }

    /**
     * Apply selected database configuration parameters from the given instance.
     *
     * <p>
     * The default {@link DatabaseConfig} is configured for transactional operation and with
     * {@linkplain EnvironmentConfig#getAllowCreate allowCreate} set to true.
     * </p>
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
     * </p>
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
     * @throws IllegalStateException if this instance is not {@linkplain #start started}
     */
    public synchronized Environment getEnvironment() {
        if (this.environment == null)
            throw new IllegalStateException("not started");
        assert this.database != null;
        return this.environment;
    }

    /**
     * Get the underlying {@link Database} associated with this instance.
     *
     * @throws IllegalStateException if this instance is not {@linkplain #start started}
     */
    public synchronized Database getDatabase() {
        if (this.environment == null)
            throw new IllegalStateException("not started");
        assert this.database != null;
        return this.database;
    }

    /**
     * Create a new transaction.
     *
     * @throws IllegalStateException if this instance is not {@linkplain #start started}
     * @throws KVDatabaseException if an unexpected error occurs
     */
    @Override
    public synchronized BerkeleyKVTransaction createTransaction() {

        // Check open
        if (this.environment == null)
            throw new IllegalStateException("not started");
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

// Lifecycle

    /**
     * Start this instance. This method must be called prior to creating any transactions.
     *
     * <p>
     * This method is idempotent.
     * </p>
     *
     * @throws IllegalStateException if this instance has already been {@linkplain #start started}
     * @throws IllegalStateException if this instance is not properly configured
     */
    @PostConstruct
    public synchronized void start() {
        if (this.environment != null) {
            assert this.database != null;
            return;
        }
        assert this.openTransactions.isEmpty();
        if (this.directory == null)
            throw new IllegalStateException("no directory configured");
        if (this.databaseName == null)
            throw new IllegalStateException("no database name configured");
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

    /**
     * Stop this instance. Does nothing if not {@linkplain #start started}.
     *
     * @throws FDBException if an error occurs
     */
    @PreDestroy
    public void stop() {

        // Mark this instance as shutdown so no new transactions are created
        final Environment oldEnvironment;
        final Database oldDatabase;
        final ArrayList<BerkeleyKVTransaction> oldTransactions;
        synchronized (this) {
            oldEnvironment = this.environment;
            oldDatabase = this.database;
            oldTransactions = new ArrayList<BerkeleyKVTransaction>(this.openTransactions);
            this.environment = null;
            this.database = null;
            this.openTransactions.clear();
        }

        // Were we already stopped?
        if (oldEnvironment == null) {
            assert this.database == null;
            return;
        }

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

