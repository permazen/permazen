
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.leveldb;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.util.ForwardingKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link LevelDBKVDatabase} transaction.
 */
public class LevelDBKVTransaction extends ForwardingKVStore implements KVTransaction, Closeable {

// Note: locking order: (1) LevelDBKVTransaction, (2) LevelDBKVDatabase

    private static final AtomicLong COUNTER = new AtomicLong();

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final long uniqueId = COUNTER.incrementAndGet();
    private final long startTime;
    private final LevelDBKVDatabase kvdb;

    // Associated snapshot and mutable view on top of it
    private final VersionInfo versionInfo;
    private final MutableView mutableView;

    private boolean closed;
    private long timeout;

    /**
     * Constructor.
     */
    LevelDBKVTransaction(LevelDBKVDatabase kvdb, VersionInfo versionInfo) {
        this.kvdb = kvdb;
        this.versionInfo = versionInfo;
        this.startTime = System.nanoTime();
        this.mutableView = new MutableView(versionInfo.getSnapshot(), true);
    }

// ForwardingKVStore

    @Override
    protected KVStore delegate() {
        this.checkState();
        return this.mutableView;
    }

// KVTransaction

    @Override
    public LevelDBKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    /**
     * Set the transaction timeout.
     *
     * <p>
     * Since {@link LevelDBKVTransaction}s do not perform any locking while the transaction is open, the configured value
     * is used instead as a timeout on the overall transaction duration. If the transaction is kept open for longer
     * than {@code timeout} milliseconds, a {@link TransactionTimeoutException} will be thrown.
     * </p>
     *
     * @param timeout transaction timeout in milliseconds, or zero for unlimited
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

// Package methods

    long getUniqueId() {
        return this.uniqueId;
    }

    VersionInfo getVersionInfo() {
        return this.versionInfo;
    }

    MutableView getMutableView() {
        return this.mutableView;
    }

    RuntimeException logException(RuntimeException e) {
        if (this.log.isDebugEnabled())
            this.log.debug("throwing exception for " + this + ": " + e);
        throw e;
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

// Internal methods

    private void checkState() {
        if (this.closed)
            throw this.logException(new StaleTransactionException(this));
        if (this.timeout == 0)
            return;
        final long time = (System.nanoTime() - this.startTime) / 1000000L;
        if (time >= this.timeout) {
            throw this.logException(new TransactionTimeoutException(this,
              "transaction has timed out after " + time + "ms > limit of " + this.timeout + "ms"));
        }
    }
}

