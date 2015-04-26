
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.mvcc.SnapshotKVDatabase;
import org.jsimpledb.kv.mvcc.SnapshotKVTransaction;
import org.jsimpledb.kv.mvcc.SnapshotVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link org.jsimpledb.kv.KVDatabase} implementation based on LevelDB and providing concurrent transactions
 * and linearizable ACID semantics.
 *
 * <p>
 * A {@linkplain #setDirectory database directory} is the only required configuration property.
 * Instances may be stopped and (re)started multiple times.
 * </p>
 *
 * <p>
 * The LevelDB Java implementation is configurable by specifying the {@link DBFactory} implementation.
 * </p>
 *
 * @see <a href="https://github.com/dain/leveldb">leveldb</a>
 * @see <a href="https://github.com/fusesource/leveldbjni">leveldbjni</a>
 */
public class LevelDBKVDatabase extends SnapshotKVDatabase {

    /**
     * Class name for the {@link DBFactory} provided by <a href="https://github.com/fusesource/leveldbjni">leveldbjni</a>.
     */
    public static final String LEVELDBJNI_CLASS_NAME = "org.fusesource.leveldbjni.JniDBFactory";

    /**
     * Class name for the {@link DBFactory} provided by <a href="https://github.com/dain/leveldb">leveldb</a>.
     */
    public static final String LEVELDB_CLASS_NAME = "org.iq80.leveldb.impl.Iq80DBFactory";

    /**
     * The name of a system property that can be set to override the default {@link DBFactory} logic.
     * Set to the name of a class that implements {@link DBFactory} and has a zero-arg constructor.
     */
    public static final String DB_FACTORY_PROPERTY = LevelDBKVDatabase.class.getName() + ".db_factory";

// Locking order: (1) LevelDBKVTransaction, (2) LevelDBKVDatabase

    private final AtomicBoolean shutdownHookRegistered = new AtomicBoolean();
    private final DBFactory factory;

    private Options options = new Options().createIfMissing(true).logger(new org.iq80.leveldb.Logger() {
        @Override
        public void log(String message) {
            LevelDBKVDatabase.this.log.info("[LevelDB] " + message);
        }
      });
    private File directory;
    private DB db;
    private boolean stopping;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * First tries the class specified by the {@link #DB_FACTORY_PROPERTY} system property, if any, then
     * <a href="https://github.com/fusesource/leveldbjni">leveldbjni</a>, and finally
     * <a href="https://github.com/dain/leveldb">leveldb</a>.
     */
    public LevelDBKVDatabase() {
        this(LevelDBKVDatabase.getDefaultDBFactory());
    }

    /**
     * Constructor.
     *
     * @param factory factory for database
     * @throws IllegalArgumentException if {@code factory} is null
     */
    public LevelDBKVDatabase(DBFactory factory) {
        if (factory == null)
            throw new IllegalArgumentException("null factory");
        this.factory = factory;
    }

    private static DBFactory getDefaultDBFactory() {

        // Get class names to try
        final ArrayList<String> classNames = new ArrayList<>(3);
        final String configuredFactoryClass = System.getProperty(DB_FACTORY_PROPERTY, null);
        if (configuredFactoryClass != null)
            classNames.add(configuredFactoryClass);
        classNames.add(LEVELDBJNI_CLASS_NAME);
        classNames.add(LEVELDB_CLASS_NAME);

        // Find class
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final Logger log = LoggerFactory.getLogger(LevelDBKVDatabase.class);
        for (String className : classNames) {
            try {
                return Class.forName(className, false, loader).asSubclass(DBFactory.class).newInstance();
            } catch (Exception e) {
                if (log.isDebugEnabled())
                    log.debug("can't load factory class `" + className + "': " + e);
                continue;
            }
        }
        throw new RuntimeException("no " + DBFactory.class.getName() + " implementation found; tried: " + classNames);
    }

// Lifecycle

    /**
     * Start this instance. This method must be called prior to creating any transactions.
     *
     * <p>
     * This method is idempotent.
     * </p>
     *
     * @throws IllegalStateException if this instance is not properly configured
     */
    @PostConstruct
    public synchronized void start() {

        // Already started?
        if (this.db != null)
            return;
        this.log.info("starting " + this);

        // Check configuration
        if (this.directory == null)
            throw new IllegalStateException("no directory configured");

        // Open database
        if (this.log.isDebugEnabled())
            this.log.debug("opening " + this + " LevelDB database");
        try {
            this.db = this.factory.open(this.directory, this.options);
        } catch (IOException e) {
            throw new RuntimeException("LevelDB database startup failed", e);
        }

        // Configure KVStore
        this.setKVStore(new LevelDBKVStore(this.db, new ReadOptions().verifyChecksums(this.options.verifyChecksums()), null));

        // Add shutdown hook so we don't leak native resources
        if (this.shutdownHookRegistered.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    LevelDBKVDatabase.this.stop();
                }
            });
        }
    }

    /**
     * Stop this instance. Does nothing if not {@linkplain #start started} or already stopped.
     */
    @PreDestroy
    public void stop() {

        // Set stopping flag to prevent new transactions from being created
        synchronized (this) {
            if (this.db == null || this.stopping)
                return;
            this.log.info("stopping " + this);
            this.stopping = true;
        }

        // Close any remaining open transactions
        this.closeTransactions();

        // Finish up
        synchronized (this) {

            // Sanity check
            assert this.db != null;

            // Close KVStore
            ((LevelDBKVStore)this.getKVStore()).close();
            this.setKVStore(null);

            // Shut down LevelDB database
            try {
                if (this.log.isDebugEnabled())
                    this.log.info("closing " + this + " LevelDB database");
                this.db.close();
            } catch (Throwable e) {
                this.log.error("caught exception closing database during shutdown (ignoring)", e);
            }

            // Reset state
            this.db = null;
            this.stopping = false;
        }
    }

// Accessors

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
     * Get the underlying {@link DB} associated with this instance.
     *
     * @return the associated {@link DB}
     * @throws IllegalStateException if this instance is not {@linkplain #start started}
     */
    public synchronized DB getDB() {
        if (this.db == null)
            throw new IllegalStateException("not started");
        return this.db;
    }

// Options

    /**
     * Get the {@link Options} this instance will use when opening the database at startup.
     *
     * @return database options
     */
    public synchronized Options getOptions() {
        return this.options;
    }

    /**
     * Set the {@link Options} this instance will use when opening the database at startup.
     * Overwrites any previous options configuration(s).
     *
     * @param options database options
     * @throws IllegalArgumentException if {@code options} is null
     */
    public synchronized void setOptions(Options options) {
        if (options == null)
            throw new IllegalArgumentException("null options");
        this.options = options;
    }

    /**
     * Configure the number of keys between restart points for delta encoding of keys. Default 16.
     *
     * @param blockRestartInterval restart interval
     */
    public synchronized void setBlockRestartInterval(int blockRestartInterval) {
        this.options = this.options.blockRestartInterval(blockRestartInterval);
    }

    /**
     * Configure the block size. Default 4K.
     *
     * @param blockSize block size
     */
    public synchronized void setBlockSize(int blockSize) {
        this.options = this.options.blockSize(blockSize);
    }

    /**
     * Configure the cache size. Default 8MB.
     *
     * @param cacheSize cache size
     */
    public synchronized void setCacheSize(long cacheSize) {
        this.options = this.options.cacheSize(cacheSize);
    }

    /**
     * Configure the compression type. Default {@link CompressionType#SNAPPY}.
     *
     * @param compressionType compression type.
     * @throws IllegalArgumentException if {@code compressionType} is null
     */
    public synchronized void setCompressionType(CompressionType compressionType) {
        this.options = this.options.compressionType(compressionType);
    }

    /**
     * Configure whether to create the database if missing. Default true.
     *
     * @param createIfMissing true to create if missing
     */
    public synchronized void setCreateIfMissing(boolean createIfMissing) {
        this.options = this.options.createIfMissing(createIfMissing);
    }

    /**
     * Configure whether to throw an error if the database already exists. Default false.
     *
     * @param errorIfExists true for error if database exists
     */
    public synchronized void setErrorIfExists(boolean errorIfExists) {
        this.options = this.options.errorIfExists(errorIfExists);
    }

    /**
     * Configure the maximum number of open files. Default 1000.
     *
     * @param maxOpenFiles maximum number of open files
     */
    public synchronized void setMaxOpenFiles(int maxOpenFiles) {
        this.options = this.options.maxOpenFiles(maxOpenFiles);
    }

    /**
     * Configure whether paranoid checks are enabled. Default false.
     *
     * @param paranoidChecks true to enable
     */
    public synchronized void setParanoidChecks(boolean paranoidChecks) {
        this.options = this.options.paranoidChecks(paranoidChecks);
    }

    /**
     * Configure whether to verify checksums. Default true.
     *
     * @param verifyChecksums true to enable
     */
    public synchronized void setVerifyChecksums(boolean verifyChecksums) {
        this.options = this.options.verifyChecksums(verifyChecksums);
    }

    /**
     * Configure the write buffer size. Default 4MB.
     *
     * @param writeBufferSize write buffer size
     */
    public synchronized void setWriteBufferSize(int writeBufferSize) {
        this.options = this.options.writeBufferSize(writeBufferSize);
    }

// KVDatabase

    /**
     * Create a new transaction.
     *
     * @throws IllegalStateException if this instance is not {@linkplain #start started}
     */
    @Override
    public synchronized LevelDBKVTransaction createTransaction() {

        // Sanity check
        if (this.db == null)
            throw new IllegalStateException("not started");
        if (this.stopping)
            throw new IllegalStateException("stop in progress");

        // OK
        return (LevelDBKVTransaction)super.createTransaction();
    }

// SnapshotKVDatabase

    @Override
    protected LevelDBKVTransaction createSnapshotKVTransaction(SnapshotVersion versionInfo) {
        return new LevelDBKVTransaction(this, versionInfo);
    }

    @Override
    protected RuntimeException wrapException(SnapshotKVTransaction tx, RuntimeException e) {
        if (e instanceof DBException)
            return new KVTransactionException(tx, "LevelDB error", e);
        return e;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[dir=" + this.directory + "]";
    }
}

