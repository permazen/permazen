
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.KVTransactionTimeoutException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.util.ByteData;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SnapshotKVDatabase} transaction.
 */
@ThreadSafe
public class SnapshotKVTransaction extends ForwardingKVStore implements KVTransaction, Closeable {

// Note: locking order: (1) SnapshotKVTransaction, (2) SnapshotKVDatabase, (3) MutableView

    private static final AtomicLong COUNTER = new AtomicLong();

    final long uniqueId = COUNTER.incrementAndGet();
    final long startTime;
    final SnapshotKVDatabase kvdb;
    final MutableView view;
    final long baseVersion;

    // Invariant: if error != null, then !db.transactions.contains(this)
    @GuardedBy("kvdb")
    volatile KVTransactionException error;

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final AtomicBoolean closed = new AtomicBoolean();   // used to detect whether commit() or rollback() has been invoked
    private final Throwable allocation;

    @GuardedBy("this")
    private boolean readOnly;
    @GuardedBy("this")
    private long timeout;
    @GuardedBy("this")
    private long commitVersion;

    /**
     * Constructor.
     *
     * @param kvdb the associated database
     * @param view mutable view to be used for this transaction
     * @param baseVersion the database version associated with {@code base}
     */
    protected SnapshotKVTransaction(SnapshotKVDatabase kvdb, MutableView view, long baseVersion) {
        Preconditions.checkArgument(kvdb != null);
        Preconditions.checkArgument(view != null);
        this.kvdb = kvdb;
        this.view = view;
        this.baseVersion = baseVersion;
        this.startTime = System.nanoTime();
        this.allocation = new Throwable("allocated here");
    }

// Accessors

    /**
     * Get the MVCC database version number on which this instance is (or was originally) based.
     *
     * @return transaction base version number
     */
    public long getBaseVersion() {
        return this.baseVersion;
    }

    /**
     * Get the MVCC database version number representing this transaction's successful commit, if any.
     *
     * @return transaction commit version number, or zero if transaction is read-only or not committed
     */
    public synchronized long getCommitVersion() {
        return this.commitVersion;
    }
    void setCommitVersion(long commitVersion) {
        assert Thread.holdsLock(this);
        assert this.commitVersion == 0;
        this.commitVersion = commitVersion;
    }

    /**
     * Get the {@link MutableView} associated with this instance.
     *
     * @return associated access and mutation state
     */
    public MutableView getMutableView() {
        return this.view;
    }

// ForwardingKVStore

    /**
     * Get the underlying {@link KVStore}.
     *
     * <p>
     * The implementation in {@link SnapshotKVTransaction} returns the {@link MutableView} associated with this instance.
     *
     * @return the underlying {@link KVStore}
     * @throws StaleKVTransactionException if this transaction is no longer valid
     * @throws KVTransactionTimeoutException if this transaction has timed out
     */
    @Override
    protected synchronized KVStore delegate() {
        this.checkAlive();
        return this.view;
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
     * than {@code timeout} milliseconds, a {@link KVTransactionTimeoutException} will be thrown.
     *
     * @param timeout transaction timeout in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    @Override
    public synchronized void setTimeout(long timeout) {
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        this.timeout = timeout;
    }

    @Override
    public synchronized ListenableFuture<Void> watchKey(ByteData key) {
        this.checkAlive();
        return this.kvdb.watchKey(key);
    }

    @Override
    public synchronized boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public synchronized void commit() {
        try {
            this.checkAlive();
        } catch (KVTransactionException e) {
            this.rollback();        // when commit() fails, rollback() is assumed, so ensure it gets done here if not already
            throw e;
        }
        this.closed.set(true);
        this.kvdb.commit(this, this.readOnly);
    }

    @Override
    public synchronized void rollback() {
        if (!this.closed.compareAndSet(false, true))
            return;
        this.kvdb.rollback(this);
    }

    @Override
    public void withWeakConsistency(Runnable action) {
        this.view.withoutReadTracking(false, action);
    }

    @Override
    public CloseableKVStore readOnlySnapshot() {
        final Writes writes;
        synchronized (this) {
            this.checkAlive();
            synchronized (this.view) {
                writes = this.view.getWrites().clone();
            }
        }
        return this.kvdb.createReadOnlySnapshot(writes);
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
          + ",base=" + this.baseVersion
          + (this.commitVersion != 0 ? ",commit=" + this.commitVersion : "")
          + (this.closed.get() ? ",closed" : "")
          + "]";
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        try {
            if (!this.closed.get()) {
                this.log.warn(this + " leaked without commit() or rollback()", this.allocation);
                this.close();
            }
        } finally {
            super.finalize();
        }
    }

// SnapshotKVDatabase Methods

    // Note this.kvdb must be locked
    void throwErrorIfAny() {
        assert Thread.holdsLock(this.kvdb);
        if (this.error != null)
            throw this.kvdb.logException(this.error.duplicate());
    }

// Internal methods

    private void checkAlive() {
        assert Thread.holdsLock(this);

        // Has commit() or rollback() already been invoked?
        if (this.closed.get())
            throw this.kvdb.logException(new StaleKVTransactionException(this));

        // Check for timeout
        if (this.error == null && this.timeout != 0) {
            final long duration = (System.nanoTime() - this.startTime) / 1000000L;
            if (duration >= this.timeout) {
                synchronized (this.kvdb) {
                    if (this.error == null) {
                        this.error = new KVTransactionTimeoutException(this,
                          "transaction has timed out after " + duration + "ms > limit of " + this.timeout + "ms");
                    }
                }
            }
        }

        // Check for error condition
        if (this.error != null) {
            synchronized (this.kvdb) {
                this.throwErrorIfAny();
            }
        }
    }
}
