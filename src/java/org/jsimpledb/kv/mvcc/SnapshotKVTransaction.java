
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mvcc;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;
import org.jsimpledb.kv.util.ForwardingKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SnapshotKVDatabase} transaction.
 */
public class SnapshotKVTransaction extends ForwardingKVStore implements KVTransaction, Closeable {

// Note: locking order: (1) SnapshotKVTransaction, (2) SnapshotKVDatabase

    private static final AtomicLong COUNTER = new AtomicLong();

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final long uniqueId = COUNTER.incrementAndGet();
    private final long startTime;
    private final SnapshotKVDatabase kvdb;
    private final SnapshotVersion versionInfo;
    private final MutableView mutableView;

    private boolean closed;
    private long timeout;

    /**
     * Constructor.
     *
     * @param kvdb the associated database
     * @param versionInfo the associated MVCC version
     */
    protected SnapshotKVTransaction(SnapshotKVDatabase kvdb, SnapshotVersion versionInfo) {
        this.kvdb = kvdb;
        this.versionInfo = versionInfo;
        this.startTime = System.nanoTime();
        this.mutableView = new MutableView(versionInfo.getSnapshot());
    }

// Accessors

    /**
     * Get the {@link MutableView} associated with this instance.
     *
     * @return associated access and mutation state
     */
    public MutableView getMutableView() {
        return this.mutableView;
    }

    /**
     * Get the MVCC version associated with this instance.
     *
     * @return associated MVCC version
     */
    public SnapshotVersion getSnapshotVersion() {
        return this.versionInfo;
    }

// ForwardingKVStore

    /**
     * Get the underlying {@link KVStore}.
     *
     * <p>
     * The implementation in {@link SnapshotKVTransaction} returns the {@link MutableView} associated with this instance.
     *
     * @return the underlying {@link KVStore}
     */
    @Override
    protected KVStore delegate() {
        this.checkState();
        return this.mutableView;
    }

// KVTransaction

    @Override
    public SnapshotKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    /**
     * Set the transaction timeout.
     *
     * <p>
     * {@link SnapshotKVTransaction}s do not perform any locking while the transaction is open. Therefore, the configured
     * value is used instead as a timeout on the overall transaction duration. If the transaction is kept open for longer
     * than {@code timeout} milliseconds, a {@link TransactionTimeoutException} will be thrown.
     * </p>
     *
     * @param timeout transaction timeout in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    @Override
    public synchronized void setTimeout(long timeout) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout < 0");
        this.timeout = timeout;
    }

    @Override
    public synchronized void commit() {
        this.checkState();
        this.closed = true;
        this.kvdb.commit(this);
    }

    @Override
    public synchronized void rollback() {
        if (this.closed)
            return;
        this.closed = true;
        this.kvdb.rollback(this);
    }

// Closeable

    /**
     * Close this instance.
     *
     * <p>
     * Equivalent to invoking {@link #rollback}.
     */
    @Override
    public void close() {
        this.rollback();
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[id=" + this.uniqueId
          + ",vers=" + this.versionInfo.getVersion()
          + (this.closed ? ",closed" : "")
          + "]";
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (!this.closed)
               this.log.warn(this + " leaked without commit() or rollback()");
            this.close();
        } finally {
            super.finalize();
        }
    }

// Internal methods

    private void checkState() {
        if (this.closed)
            throw this.kvdb.logException(new StaleTransactionException(this));
        if (this.timeout == 0)
            return;
        final long time = (System.nanoTime() - this.startTime) / 1000000L;
        if (time >= this.timeout) {
            throw this.kvdb.logException(new TransactionTimeoutException(this,
              "transaction has timed out after " + time + "ms > limit of " + this.timeout + "ms"));
        }
    }
}

