
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.caching;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.util.MovingAverage;

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
 * @see CachingKVStore
 */
public class CachingKVDatabase extends AbstractCachingConfig implements KVDatabase {

    /**
     * Default for the initial round-trip time estimate in milliseconds.
     *
     * @see #setInitialRttEstimate
     */
    public static final int DEFAULT_INITIAL_RTT_ESTIMATE_MILLIS = 50;

    /**
     * Default thread pool size when no {@link ExecutorService} is explicitly configured.
     *
     * @see #setExecutorService
     */
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;

    private static final double RTT_ESTIMATE_DECAY_FACTOR = 0.025;

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    private KVDatabase inner;
    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    private long initialRttEstimate = TimeUnit.MILLISECONDS.toNanos(DEFAULT_INITIAL_RTT_ESTIMATE_MILLIS);
    private ExecutorService executor;

    private boolean started;
    private boolean privateExecutor;
    private MovingAverage rtt;

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
     * Get the initial round trip time estimate.
     *
     * <p>
     * Default is {@value #DEFAULT_INITIAL_RTT_ESTIMATE_MILLIS} ms.
     *
     * @return initial round trip time estimate in nanoseconds
     */
    public synchronized long getInitialRttEstimate() {
        return this.initialRttEstimate;
    }

    /**
     * Set the initial round trip time estimate.
     *
     * <p>
     * This is just an initial estimate. The actual value used adjusts dynamically as transactions occur
     * and actual RTT measurements are made.
     *
     * <p>
     * Default is {@value #DEFAULT_INITIAL_RTT_ESTIMATE_MILLIS} ms.
     *
     * @param initialRttEstimate initial round trip time estimate in nanoseconds
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if {@code initialRttEstimate < 0}
     */
    public synchronized void setInitialRttEstimate(long initialRttEstimate) {
        Preconditions.checkArgument(initialRttEstimate >= 0, "initialRttEstimate < 0");
        Preconditions.checkState(!this.started, "already started");
        this.initialRttEstimate = initialRttEstimate;
    }

    /**
     * Get the inner {@link KVDatabase}.
     *
     * @return the underlying {@link KVDatabase}
     */
    public synchronized KVDatabase getKVDatabase() {
        return this.inner;
    }

    /**
     * Configure the underlying {@link KVDatabase}.
     *
     * @param inner the underlying {@link KVDatabase}
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setKVDatabase(KVDatabase inner) {
        Preconditions.checkState(!this.started, "already started");
        this.inner = inner;
    }

    /**
     * Get the configured {@link ExecutorService}, if any.
     *
     * @return caller-supplied executor to use for background tasks, or null for none
     */
    public synchronized ExecutorService getExecutorService() {
        return this.executor;
    }

    /**
     * Configure a {@link ExecutorService} to use for asynchronous queries to the underlying database.
     *
     * <p>
     * This property is optional; if none is configured, a private thread pool will be
     * automatically created and setup internally by {@link #start} and torn down by {@link #stop}.
     * If an outside executor is configured here, then it will <i>not</i> be shutdown by {@link #stop}.
     *
     * @param executor caller-supplied executor to use for background tasks, or null for none
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setExecutorService(ExecutorService executor) {
        Preconditions.checkState(!this.started, "already started");
        this.executor = executor;
    }

    /**
     * Get the number of threads in the internally-created thread pool.
     *
     * <p>
     * This property is ignored if a custom {@link ExecutorService} is configured via
     * {@link #setExecutorService setExecutorService()}.
     *
     * <p>
     * Default value is {@value #DEFAULT_THREAD_POOL_SIZE}.
     *
     * @return number of threads in thread pool
     */
    public synchronized int getThreadPoolSize() {
        return this.threadPoolSize;
    }

    /**
     * Set the number of threads in the internally-created thread pool.
     *
     * <p>
     * This property is ignored if a custom {@link ExecutorService} is configured via
     * {@link #setExecutorService setExecutorService()}.
     *
     * <p>
     * Default value is {@value #DEFAULT_THREAD_POOL_SIZE}.
     *
     * @param threadPoolSize number of threads in thread pool
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if {@code threadPoolSize <= 0}
     */
    public synchronized void setThreadPoolSize(int threadPoolSize) {
        Preconditions.checkArgument(threadPoolSize > 0, "threadPoolSize <= 0");
        Preconditions.checkState(!this.started, "already started");
        this.threadPoolSize = threadPoolSize;
    }

// Lifecycle

    @Override
    @PostConstruct
    public synchronized void start() {
        Preconditions.checkState(this.inner != null, "no inner KVDatabase configured");
        if (this.started)
            return;
        this.privateExecutor = this.executor == null;
        if (this.privateExecutor) {
            this.executor = Executors.newFixedThreadPool(this.threadPoolSize, r -> {
                final Thread thread = new Thread(r);
                thread.setName(this.getClass().getSimpleName() + "-" + THREAD_COUNTER.incrementAndGet());
                return thread;
            });
        }
        this.rtt = new MovingAverage(RTT_ESTIMATE_DECAY_FACTOR, this.initialRttEstimate);
        try {
            this.inner.start();
            this.started = true;
        } finally {
            if (!this.started)
                this.shutdown();
        }
    }

    @Override
    @PreDestroy
    public synchronized void stop() {
        if (!this.started)
            return;
        this.shutdown();
        this.started = false;
    }

    private synchronized void shutdown() {
        if (this.executor != null) {
            if (this.privateExecutor)
                this.executor.shutdown();
            this.executor = null;
        }
        this.inner.stop();
    }

// Transactions

    @Override
    public CachingKVTransaction createTransaction() {
        return this.createTransaction(this.inner::createTransaction);
    }

    @Override
    public CachingKVTransaction createTransaction(Map<String, ?> options) {
        return this.createTransaction(() -> this.inner.createTransaction(options));
    }

    protected synchronized CachingKVTransaction createTransaction(Supplier<? extends KVTransaction> innerTxCreator) {
        Preconditions.checkState(this.started, "not started");
        return new CachingKVTransaction(this, innerTxCreator.get(), this.executor, (long)this.rtt.get());
    }

// RTT estimate

    /**
     * Get the current round trip time estimate.
     *
     * @return current RTT estimate in nanoseconds
     * @throws IllegalStateException if this instance has never {@link #start}ed
     */
    public synchronized double getRttEstimate() {
        Preconditions.checkState(this.rtt != null, "instance has never started");
        return this.rtt.get();
    }

    synchronized void updateRttEstimate(double rtt) {
        this.rtt.add(rtt);
    }
}
