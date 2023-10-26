
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.xodus;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.util.HashSet;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KVDatabase} view of an Xodus database.
 *
 * <p>
 * A {@linkplain #setDirectory database directory} is the only required configuration property.
 * Instances may be stopped and (re)started multiple times.
 *
 * <p>
 * Transactions implement {@link XodusKVTransaction#mutableSnapshot}.
 */
@ThreadSafe
public class XodusKVDatabase implements KVDatabase {

    // Lock order: (1) XodusKVTransaction, (2) XodusKVDatabase

    /**
     * Default Xodus store name ({@value #DEFAULT_STORE_NAME}).
     *
     * @see #setStoreName
     */
    public static final String DEFAULT_STORE_NAME = "permazen";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    @GuardedBy("this")
    private File directory;
    @GuardedBy("this")
    private String storeName = DEFAULT_STORE_NAME;
    @GuardedBy("this")
    private EnvironmentConfig config = EnvironmentConfig.DEFAULT;
    @GuardedBy("this")
    private Environment env;
    @GuardedBy("this")
    private final HashSet<XodusKVTransaction> openTx = new HashSet<>();

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
     * Get the Xodus store name to use.
     *
     * @return configured store name
     */
    public synchronized String getStoreName() {
        return this.storeName;
    }

    /**
     * Set the Xodus store name to use.
     *
     * <p>
     * Default value is {@link #DEFAULT_STORE_NAME}.
     *
     * @param storeName store name
     * @throws IllegalStateException if this instance is already {@link #start}ed
     * @throws IllegalArgumentException if {@code storeName} is null
     */
    public synchronized void setStoreName(String storeName) {
        Preconditions.checkArgument(storeName != null, "null storeName");
        Preconditions.checkState(this.env == null, "already started");
        this.storeName = storeName;
    }

    /**
     * Get the {@link EnvironmentConfig} that will be used when creating the associated {@link Environment}.
     *
     * @return environment config
     */
    public synchronized EnvironmentConfig getEnvironmentConfig() {
        return this.config;
    }

    /**
     * Set the {@link EnvironmentConfig} to be used when creating the associated {@link Environment}.
     *
     * <p>
     * Default value is {@link EnvironmentConfig#DEFAULT}.
     *
     * @param config environment config
     * @throws IllegalStateException if this instance is already {@link #start}ed
     * @throws IllegalArgumentException if {@code config} is null
     */
    public synchronized void setEnvironmentConfig(EnvironmentConfig config) {
        Preconditions.checkState(this.env == null, "already started");
        this.config = config;
    }

// Accessors

    /**
     * Get the underlying {@link Environment} associated with this instance.
     *
     * @return the associated {@link Environment}
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    public synchronized Environment getEnvironment() {
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

        // Open database
        if (this.log.isDebugEnabled())
            this.log.debug("starting Xodus database {}", this);
        this.env = Environments.newInstance(this.directory, this.config);
    }

    @Override
    @PreDestroy
    public void stop() {
        XodusKVTransaction[] cleanups = new XodusKVTransaction[0];
        while (true) {

            // Close any remaining transactions - otherwise this.env.close() will throw an exception - but not while locked
            for (XodusKVTransaction tx : cleanups)
                tx.rollback();

            // Shutdown when no open transactions remain
            synchronized (this) {

                // Already stopped?
                if (this.env == null)
                    return;

                // Any remaining open transactions? If so close them and restart
                cleanups = this.openTx.toArray(new XodusKVTransaction[this.openTx.size()]);
                this.openTx.clear();
                if (cleanups.length > 0)
                    continue;

                // Shut down Xodus database
                this.log.info("stopping {}", this);
                try {
                    if (this.log.isDebugEnabled())
                        this.log.debug("stopping Xodus database {}", this);
                    this.env.close();
                } catch (Throwable e) {
                    this.log.error("caught exception closing database during shutdown (ignoring)", e);
                }
                this.env = null;
                return;
            }
        }
    }

    synchronized void transactionClosed(XodusKVTransaction tx) {
        this.openTx.remove(tx);
    }

// KVDatabase

    @Override
    public synchronized XodusKVTransaction createTransaction() {
        return this.createTransaction(null);
    }

    @Override
    public synchronized XodusKVTransaction createTransaction(Map<String, ?> options) {
        Preconditions.checkState(this.env != null, "not started");
        final XodusKVTransaction tx = new XodusKVTransaction(this);
        this.openTx.add(tx);
        return tx;
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #stop} to close any unclosed iterators.
     */
    @Override
    @SuppressWarnings("deprecation")
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
          + ",store=\"" + this.storeName + "\""
          + ",env=" + this.env
          + "]";
    }
}
