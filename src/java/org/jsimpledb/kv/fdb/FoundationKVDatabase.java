
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.fdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.FDBException;
import com.foundationdb.NetworkOptions;
import com.google.common.base.Preconditions;

import java.util.concurrent.Executor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVDatabaseException;

/**
 * FoundationDB {@link KVDatabase} implementation.
 *
 * <p>
 * Allows specifying a {@linkplain #setKeyPrefix key prefix} for all keys, allowing multiple independent databases.
 * </p>
 */
public class FoundationKVDatabase implements KVDatabase {

    /**
     * The API version used by this class.
     */
    public static final int API_VERSION = 300;

    private final FDB fdb = FDB.selectAPIVersion(API_VERSION);
    private final NetworkOptions options = this.fdb.options();

    private String clusterFilePath;
    private byte[] databaseName = new byte[] { (byte)'D', (byte)'B' };
    private byte[] keyPrefix;
    private Executor executor;

    private Database database;
    private boolean started;                                // FDB can only be started up once

    /**
     * Constructor.
     *
     * @throws FDBException if {@link #API_VERSION} is not supported
     */
    public FoundationKVDatabase() {
    }

    /**
     * Get the {@link NetworkOptions} associated with this instance.
     * Options must be configured prior to {@link #start}.
     *
     * @return network options
     */
    public NetworkOptions getNetworkOptions() {
        return this.options;
    }

    /**
     * Configure the {@link Executor} used for the FoundationDB networking event loop.
     *
     * <p>
     * By default, the default thread pool is used to execute the FoundationDB network.
     * </p>
     *
     * @param executor executor for networking activity
     * @see FDB#startNetwork(Executor) FDB.startNetwork()
     */
    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    /**
     * Configure the cluster file path. Default is null, which results in the default fdb.cluster file being used.
     *
     * @param clusterFilePath cluster file pathname
     */
    public void setClusterFilePath(String clusterFilePath) {
        this.clusterFilePath = clusterFilePath;
    }

    /**
     * Configure the database name. Currently the default value ({@code "DB".getBytes()}) is the only valid value.
     *
     * @param databaseName database name
     */
    public void setDatabaseName(byte[] databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Get the key prefix for all keys.
     *
     * @return key prefix, or null if there is none configured
     */
    public byte[] getKeyPrefix() {
        return this.keyPrefix.clone();
    }

    /**
     * Configure a prefix for all keys. The prefix will be added/removed automatically with all access.
     * The default prefix is null, which is equivalent to an empty prefix.
     *
     * <p>
     * The key prefix may not be changed after this instance has {@linkplain #start started}.
     * </p>
     *
     * @param keyPrefix new prefix, or null for none
     * @throws IllegalArgumentException if {@code keyPrefix} starts with {@code 0xff}
     * @throws IllegalStateException if this instance has already been {@linkplain #start started}
     */
    public void setKeyPrefix(byte[] keyPrefix) {
        Preconditions.checkState(this.database == null, "already started");
        Preconditions.checkArgument(keyPrefix == null || keyPrefix.length == 0 || keyPrefix[0] != (byte)0xff,
          "prefix starts with 0xff");
        this.keyPrefix = keyPrefix != null && keyPrefix.length > 0 ? keyPrefix.clone() : null;
    }

    /**
     * Get the underlying {@link Database} associated with this instance.
     *
     * @return the associated {@link Database}
     * @throws IllegalStateException if this instance has not yet been {@linkplain #start started}
     */
    public Database getDatabase() {
        Preconditions.checkState(this.database != null, "not started");
        return this.database;
    }

// KVDatabase

    @Override
    @PostConstruct
    public synchronized void start() {
        if (this.database != null)
            return;
        if (this.started)
            throw new UnsupportedOperationException("restarts not supported");
        this.database = this.fdb.open(this.clusterFilePath, this.databaseName);
        if (this.executor != null)
            this.fdb.startNetwork(this.executor);
        else
            this.fdb.startNetwork();
        this.started = true;
    }

    @Override
    @PreDestroy
    public synchronized void stop() {
        if (this.database == null)
            return;
        this.fdb.stopNetwork();
        this.database = null;
    }

    @Override
    public FoundationKVTransaction createTransaction() {
        Preconditions.checkState(this.database != null, "not started");
        try {
            return new FoundationKVTransaction(this, this.keyPrefix);
        } catch (FDBException e) {
            throw new KVDatabaseException(this, e);
        }
    }
}

