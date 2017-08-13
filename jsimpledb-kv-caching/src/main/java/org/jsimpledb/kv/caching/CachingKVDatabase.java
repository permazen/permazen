
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.caching;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jsimpledb.kv.KVDatabase;

/**
 * A wrapper around an inner {@link KVDatabase} that adds a caching layer to transactions by wrapping them
 * in a {@link CachingKVStore}.
 *
 * <p>
 * See {@link CachingKVStore} for details on how caching is performed.
 *
 * <p><b>Consistency Assumptions</b></p>
 *
 * <p>
 * <b>Warning:</b> this class assumes that the underlying {@link KVDatabase} provides transactions guaranteeing
 * fully consistent reads: the data returned by any two read operations, no matter when they occur, must be consistent
 * within any given transaction. A corollary is that transactions must be fully isolated from each other.
 * Enabling assertions on this package may detect some violations of this assumption.
 *
 * @see KVTransaction
 */
public class CachingKVDatabase extends AbstractCachingConfig implements KVDatabase {

    /**
     * Default for round-trip time estimate in milliseconds.
     *
     * @see #setRttEstimate
     */
    public static final int DEFAULT_RTT_MILLIS = 200;

    /**
     * Default thread pool size when no {@link ExecutorService} is explicitly configured.
     *
     * @see #setExecutorService
     */
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;

    private KVDatabase inner;
    private int rttEstimate;
    private ExecutorService executor;

    private boolean started;
    private boolean privateExecutor;

    /**
     * Default constructor.
     *
     * <p>
     * Instances must be configured with an inner {@link KVDatabase} before starting.
     */
    public CachingKVDatabase() {
    }

    /**
     * Primary constructor.
     *
     * @param inner inner {@link KVDatabase}
     */
    public CachingKVDatabase(KVDatabase inner) {
        this.inner = inner;
    }

// Accessors

    /**
     * Get the estimated round trip time.
     *
     * <p>
     * Default is {@value #DEFAULT_RTT_MILLIS} ms.
     *
     * @return estimated round trip time in millseconds
     */
    public synchronized int getRttEstimate() {
        return this.rttEstimate;
    }

    /**
     * Set the estimated round trip time.
     *
     * <p>
     * Default is {@value #DEFAULT_RTT_MILLIS} ms.
     *
     * @param rttEstimate estimated round trip time in millseconds
     * @throws IllegalArgumentException if {@code rttEstimate < 0}
     */
    public synchronized void setRttEstimate(int rttEstimate) {
        this.rttEstimate = rttEstimate;
    }

    /**
     * Get the inner {@link KVDatabase}.
     */
    public synchronized KVDatabase getKVDatabase() {
        return this.inner;
    }

    /**
     * Configure the inner {@link KVDatabase}.
     *
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setKVDatabase(KVDatabase inner) {
        Preconditions.checkState(!this.started, "already started");
        this.inner = inner;
    }

    /**
     * Configure a {@link ExecutorService} to use for asynchronous queries to the underlying database.
     *
     * <p>
     * This property is optional; if none is configured, a private thread pool will be used.
     *
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setExecutorService(ExecutorService executor) {
        Preconditions.checkState(!this.started, "already started");
        this.executor = executor;
    }

    @Override
    public synchronized void start() {
        Preconditions.checkState(this.inner != null, "no inner KVDatabase configured");
        if (this.started)
            return;
        this.privateExecutor = this.executor == null;
        if (this.privateExecutor)
            this.executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
        this.inner.start();
        this.started = true;
    }

    @Override
    public synchronized void stop() {
        if (!this.started)
            return;
        if (this.privateExecutor) {
            this.executor.shutdown();
            this.executor = null;
        }
        this.inner.stop();
        this.started = false;
    }

    @Override
    public synchronized CachingKVTransaction createTransaction() {
        Preconditions.checkState(this.started, "not started");
        return new CachingKVTransaction(this, this.inner.createTransaction(), this.executor, this.rttEstimate);
    }

    @Override
    public synchronized CachingKVTransaction createTransaction(Map<String, ?> options) {
        Preconditions.checkState(this.started, "not started");
        return new CachingKVTransaction(this, this.inner.createTransaction(options), this.executor, this.rttEstimate);
    }
}
