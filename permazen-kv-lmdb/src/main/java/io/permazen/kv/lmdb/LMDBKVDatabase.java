
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.lmdb;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link io.permazen.kv.KVDatabase} view of an LMDB database.
 *
 * <p>
 * A {@linkplain #setDirectory database directory} is the only required configuration property.
 * Instances may be stopped and (re)started multiple times.
 *
 * @param <T> buffer type
 */
@ThreadSafe
public abstract class LMDBKVDatabase<T> implements KVDatabase {

    // Lock order: (1) LMDBKVTransaction, (2) LMDBKVDatabase

    /**
     * Default LMDB database name ({@value #DEFAULT_DATABASE_NAME}).
     *
     * @see #setDatabaseName
     */
    public static final String DEFAULT_DATABASE_NAME = "permazen";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    @GuardedBy("this")
    private File directory;
    @GuardedBy("this")
    private String databaseName = DEFAULT_DATABASE_NAME;
    @GuardedBy("this")
    private Env.Builder<T> builder;
    @GuardedBy("this")
    private EnumSet<EnvFlags> flags = EnumSet.noneOf(EnvFlags.class);
    @GuardedBy("this")
    private Env<T> env;
    @GuardedBy("this")
    private Dbi<T> db;
    @GuardedBy("this")
    private final HashSet<LMDBKVTransaction<T>> openTx = new HashSet<>();

// Constructors

    /**
     * Constructor.
     *
     * @param defaultBuilder the default builder
     * @throws IllegalArgumentException if {@code defaultBuilder} is null
     */
    protected LMDBKVDatabase(Env.Builder<T> defaultBuilder) {
        Preconditions.checkArgument(defaultBuilder != null, "null defaultBuilder");
        this.builder = defaultBuilder;
    }

// Configuration

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
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setDirectory(File directory) {
        Preconditions.checkState(this.env == null, "already started");
        this.directory = directory;
    }

    /**
     * Get the LMDB database name to use.
     *
     * @return configured database name
     */
    public synchronized String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Set the LMDB database name to use.
     *
     * <p>
     * Default value is {@link #DEFAULT_DATABASE_NAME}.
     *
     * @param databaseName database name
     * @throws IllegalStateException if this instance is already {@link #start}ed
     * @throws IllegalArgumentException if {@code databaseName} is null
     */
    public synchronized void setDatabaseName(String databaseName) {
        Preconditions.checkState(this.env == null, "already started");
        this.databaseName = databaseName;
    }

    /**
     * Get the {@link EnvFlags} that will be used when opening the associated {@link Env}.
     *
     * @return environment flags
     */
    public synchronized EnumSet<EnvFlags> getEnvFlags() {
        return this.flags.clone();
    }

    /**
     * Set the flags to be used when opening the associated {@link Env}.
     *
     * <p>
     * Default is empty set.
     *
     * @param flags environment flags
     * @throws IllegalStateException if this instance is already {@link #start}ed
     * @throws IllegalArgumentException if {@code flags} is null
     */
    public synchronized void setEnvBuilder(EnumSet<EnvFlags> flags) {
        Preconditions.checkArgument(flags != null, "null flags");
        Preconditions.checkState(this.env == null, "already started");
        this.flags = flags.clone();
    }

    /**
     * Get the {@link Env.Builder} that will be used when opening the associated {@link Env}.
     *
     * @return environment builder
     */
    public synchronized Env.Builder<T> getEnvBuilder() {
        return this.builder;
    }

    /**
     * Set a custom builder to be used when opening the associated {@link Env}.
     *
     * @param builder environment builder
     * @throws IllegalStateException if this instance is already {@link #start}ed
     * @throws IllegalArgumentException if {@code builder} is null
     */
    public synchronized void setEnvBuilder(Env.Builder<T> builder) {
        Preconditions.checkArgument(builder != null, "null builder");
        Preconditions.checkState(this.env == null, "already started");
        this.builder = builder;
    }

// Accessors

    /**
     * Get the {@link Env} associated with this instance.
     *
     * @return the associated {@link Env}
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    public synchronized Env<T> getEnv() {
        Preconditions.checkState(this.env != null, "not started");
        return this.env;
    }

// Lifecycle

    @Override
    @PostConstruct
    public synchronized void start() {

        // Already started?
        if (this.env != null)
            return;
        this.log.info("starting {}", this);

        // Check configuration
        Preconditions.checkState(this.directory != null, "no directory configured");
        if (!this.directory.exists() && !this.directory.mkdirs())
            throw new RuntimeException("failed to create directory " + this.directory);
        if (!this.directory.isDirectory())
            throw new RuntimeException("file " + this.directory + " is not a directory");

        // Open environment
        if (this.log.isDebugEnabled())
            this.log.debug("starting LMDB database {}", this);
        this.env = this.builder.open(this.directory, this.flags.toArray(new EnvFlags[this.flags.size()]));

        // Open database
        boolean ok = false;
        try {
            this.db = this.env.openDbi(this.databaseName, DbiFlags.MDB_CREATE);
            ok = true;
        } finally {
            if (!ok)
                this.env.close();
        }
    }

    @Override
    @PreDestroy
    public void stop() {
        final ArrayList<LMDBKVTransaction<T>> cleanups = new ArrayList<>();
        while (true) {

            // Close any remaining transactions - otherwise this.env.close() will throw an exception - but not while locked
            for (LMDBKVTransaction<T> tx : cleanups)
                tx.rollback();

            // Shutdown when no open transactions remain
            synchronized (this) {

                // Already stopped?
                if (this.env == null)
                    return;

                // Any remaining open transactions? If so close them and restart
                cleanups.clear();
                cleanups.addAll(this.openTx);
                this.openTx.clear();
                if (!cleanups.isEmpty())
                    continue;

                // Shut down LMDB database
                this.log.info("stopping {}", this);
                try {
                    if (this.log.isDebugEnabled())
                        this.log.debug("stopping LMDB database {}", this);
                    //this.db.close();      // we're not supposed to use this
                    this.env.close();
                } catch (Throwable e) {
                    this.log.error("caught exception closing database during shutdown (ignoring)", e);
                }
                this.env = null;
                return;
            }
        }
    }

    synchronized void transactionClosed(LMDBKVTransaction<T> tx) {
        this.openTx.remove(tx);
    }

// KVDatabase

    @Override
    public synchronized LMDBKVTransaction<T> createTransaction() {
        return this.createTransaction(null);
    }

    @Override
    public synchronized LMDBKVTransaction<T> createTransaction(Map<String, ?> options) {
        Preconditions.checkState(this.env != null, "not started");
        final LMDBKVTransaction<T> tx = this.doCreateTransaction(this.env, this.db, options);
        this.openTx.add(tx);
        return tx;
    }

    protected abstract LMDBKVTransaction<T> doCreateTransaction(Env<T> env, Dbi<T> db, Map<String, ?> options);

// Object

    /**
     * Finalize this instance. Invokes {@link #stop} to close any unclosed iterators.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            if (this.env != null)
               this.log.warn(this + " leaked without invoking stop()");
            this.stop();
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[dir=" + this.directory
          + ",database=\"" + this.databaseName + "\""
          + ",env=" + this.env
          + "]";
    }
}
