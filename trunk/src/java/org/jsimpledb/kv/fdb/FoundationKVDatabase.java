
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.fdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.FDBException;
import com.foundationdb.NetworkOptions;

import java.util.concurrent.Executor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVDatabaseException;

/**
 * FoundationDB {@link KVDatabase} implementation.
 */
public class FoundationKVDatabase implements KVDatabase {

    /**
     * The API version used by this class.
     */
    public static final int API_VERSION = 200;

    private final FDB fdb = FDB.selectAPIVersion(API_VERSION);
    private final NetworkOptions options = this.fdb.options();

    private String clusterFilePath;
    private byte[] databaseName = "DB".getBytes();
    private Executor executor;

    private Database database;

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
     */
    public NetworkOptions getNetworkOptions() {
        return this.options;
    }

    /**
     * Configure the {@link Executor} used for the FoundationDB networking event loop.
     */
    public void setExecutor() {
        this.executor = executor;
    }

    /**
     * Configure the cluster file path. Default is null, which results in the default fdb.cluster file being used.
     */
    public void setClusterFilePath() {
        this.clusterFilePath = clusterFilePath;
    }

    /**
     * Configure the database name. Currently the default value ({@code "DB".getBytes()}) is the only valid value.
     */
    public void setDatabaseName() {
        this.databaseName = databaseName;
    }

    /**
     * Get the underlying {@link Database} associated with this instance.
     *
     * @throws IllegalStateException if this instance has not yet been {@linkplain #start started}
     */
    public Database getDatabase() {
        if (this.database == null)
            throw new IllegalStateException("not started");
        return this.database;
    }

    /**
     * Start this instance. This method must be called prior to creating any transactions.
     *
     * <p>
     * This method is idempotent.
     * </p>
     *
     * @throws IllegalStateException if this instance has already been {@linkplain #start started}
     * @throws IllegalStateException if this instance has already been {@linkplain #stop stopped}
     */
    @PostConstruct
    public synchronized void start() {
        if (this.database != null)
            throw new IllegalStateException("already started");
        this.database = this.fdb.open(this.clusterFilePath, this.databaseName);
        if (this.executor != null)
            this.fdb.startNetwork(this.executor);
        else
            this.fdb.startNetwork();
    }

    /**
     * Stop this instance. Invokes {@link FDB#stopNetwork}. Does nothing if not {@linkplain #start started}.
     *
     * @throws FDBException if an error occurs
     */
    @PreDestroy
    public synchronized void stop() {
        if (this.database == null)
            return;
        this.fdb.stopNetwork();
    }

    /**
     * Create a new transaction.
     *
     * @throws IllegalStateException if this instance has not yet been {@linkplain #start started}
     * @throws IllegalStateException if this instance has already been {@linkplain #stop stopped}
     * @throws KVDatabaseException if an unexpected error occurs
     */
    @Override
    public FoundationKVTransaction createTransaction() {
        if (this.database == null)
            throw new IllegalStateException("not started");
        try {
            return new FoundationKVTransaction(this);
        } catch (FDBException e) {
            throw new KVDatabaseException(this, e);
        }
    }
}

