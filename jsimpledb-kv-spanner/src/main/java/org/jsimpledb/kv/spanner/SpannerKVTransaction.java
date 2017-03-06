
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.spanner;

import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Timestamp;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.concurrent.Future;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.util.BatchingKVStore;
import org.jsimpledb.kv.util.ForwardingKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SpannerKVDatabase} transaction.
 */
@ThreadSafe
public class SpannerKVTransaction extends ForwardingKVStore implements KVTransaction {

    private enum State {
        INITIAL,                    // transaction is open, but no data has been accessed yet
        ACCESSED,                   // transaction is open, and some data has been queried from spanner
        CLOSED                      // transaction is closed
    };

    private static final int INITIAL_RTT_ESTIMATE = 50;                                 // 50 ms

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final SpannerKVDatabase kvdb;
    protected final DatabaseClient client;
    protected final String tableName;
    protected final TimestampBound consistency;

    @GuardedBy("this")
    private boolean readOnly;
    @GuardedBy("this")
    private ReadContext context;
    @GuardedBy("this")
    private ReadWriteSpannerView view;
    @GuardedBy("this")
    private BatchingKVStore batcher;
    @GuardedBy("this")
    private State state = State.INITIAL;

    /**
     * Constructor.
     *
     * @param kvdb associated database
     * @param client client for access
     * @param tableName Spanner key/value database table name
     * @param consistency transaction consistency level
     * @throws IllegalArgumentException if any paramter is null
     */
    protected SpannerKVTransaction(SpannerKVDatabase kvdb, DatabaseClient client, String tableName, TimestampBound consistency) {
        Preconditions.checkArgument(kvdb != null);
        Preconditions.checkArgument(client != null);
        Preconditions.checkArgument(tableName != null);
        Preconditions.checkArgument(consistency != null);
        this.kvdb = kvdb;
        this.client = client;
        this.tableName = tableName;
        this.consistency = consistency;
        this.readOnly = !this.isStrongConsistency();
    }

// Accessors

    /**
     * Get the consistency level configured for this transaction.
     *
     * <p>
     * Note that read-write transactions always use strong consistency.
     *
     * @return consistency of this transaction
     */
    public synchronized TimestampBound getConsistency() {
        return this.consistency;
    }

    /**
     * Convenience method to determine whether this transaction is using strong consistency.
     *
     * @return true if this transaction has strong consistency
     */
    public synchronized boolean isStrongConsistency() {
        return this.consistency.getMode().equals(TimestampBound.Mode.STRONG);
    }

    /**
     * Get the timestamp associated with this transaction.
     *
     * <p>
     * For read-only transactions, this returns the Spanner timestamp at which the data was accessed.
     * It should not be invoked until at least one data query has occurred.
     *
     * <p>
     * For read-write transactions, this returns the Spanner timestamp at which the changes were applied.
     * It should not be invoked until the transaction is committed.
     *
     * @return this transaction's Spanner timestamp
     * @throws IllegalStateException if timestamp is not available yet
     */
    public synchronized Timestamp getTimestamp() {
        try {
            switch (this.state) {
            case INITIAL:
                throw new IllegalStateException("no data has been accessed yet");
            case ACCESSED:
                if (this.context instanceof TransactionContext)
                    throw new IllegalStateException("transaction is not committed yet");
                break;
            case CLOSED:
            default:
                break;
            }
            if (this.context instanceof TransactionContext)
                return (Timestamp)Access.invoke(Access.TRANSACTION_CONTEXT_COMMIT_TIMESTAMP_METHOD, this.context);
            if (this.context instanceof ReadOnlyTransaction)
                return ((ReadOnlyTransaction)this.context).getReadTimestamp();
            return null;
        } catch (SpannerException e) {
            throw this.wrapException(e);
        }
    }

// KVTransaction

    @Override
    public SpannerKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    /**
     * Set transaction timeout.
     *
     * <p>
     * Currently not supported; this method does nothing.
     */
    @Override
    public void setTimeout(long timeout) {
        // ignore - not supported
    }

    @Override
    public synchronized boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        Preconditions.checkState(this.state == State.INITIAL || readOnly == this.readOnly, "data already accessed");
        Preconditions.checkArgument(this.isStrongConsistency() || readOnly,
          "strong consistency is required for read-write transactions");
        this.readOnly = readOnly;
    }

    @Override
    public synchronized void commit() {

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("commit() invoked: state=" + this.state + " view=" + this.view);

        // Check state
        switch (this.state) {
        case INITIAL:
            assert this.view == null;
            assert this.batcher == null;
            assert this.context == null;
            this.state = State.CLOSED;
            return;
        case ACCESSED:
            break;
        case CLOSED:
        default:
            throw new StaleTransactionException(this);
        }

        // Commit transaction (if read/write) and close view
        try {
            if (this.context instanceof TransactionContext) {

                // Transfer mutations into the transaction context
                this.view.bufferMutations((TransactionContext)this.context);

                // Commit transaction
                if (this.log.isTraceEnabled())
                    this.log.trace("committing transaction " + this.context);
                Access.invoke(Access.TRANSACTION_CONTEXT_COMMIT_METHOD, this.context);
            }
        } catch (SpannerException e) {
            throw this.wrapException(e);
        } finally {
            this.cleanup();
        }
    }

    @Override
    public synchronized void rollback() {

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("rollback() invoked: state=" + this.state + " view=" + this.view);

        // Check state
        switch (this.state) {
        case INITIAL:
            assert this.view == null;
            assert this.context == null;
            this.state = State.CLOSED;
            return;
        case ACCESSED:
            break;
        case CLOSED:
        default:
            return;
        }

        // Rollback transaction (if read/write) and close view
        try {
            if (this.context instanceof TransactionContext)
                Access.invoke(Access.TRANSACTION_CONTEXT_ROLLBACK_METHOD, this.context);
        } catch (SpannerException e) {
            if (this.log.isDebugEnabled())
                this.log.debug("got exception during rollback (ignoring)", e);
        } finally {
            this.cleanup();
        }
    }

    private void cleanup() {
        try {
            this.view.close();
        } finally {
            try {
                if (this.batcher != null)
                    this.batcher.close();
            } finally {
                this.view = null;
                this.batcher = null;
                this.context = null;
                this.state = State.CLOSED;
            }
        }
    }

    /**
     * Set key watch.
     *
     * <p>
     * Key watches are not supported.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Future<Void> watchKey(byte[] key) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a mutable snapshot of this transaction.
     *
     * <p>
     * This method is not supported.
     *
     * <p>
     * With Spanner, a transaction is not needed to create mutable snapshots; instead, see
     * {@link SpannerKVDatabase#snapshot SpannerKVDatabase.snapshot()} and {@link org.jsimpledb.kv.mvcc.MutableView}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public CloseableKVStore mutableSnapshot() {
        throw new UnsupportedOperationException();
    }

// ForwardingKVStore

    @Override
    public byte[] get(byte[] key) {
        try {
            return super.get(key);
        } catch (SpannerException e) {
            this.rollback();
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        try {
            return super.getAtLeast(minKey, maxKey);
        } catch (SpannerException e) {
            this.rollback();
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        try {
            return super.getAtMost(maxKey, minKey);
        } catch (SpannerException e) {
            this.rollback();
            throw this.wrapException(e);
        }
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        try {
            return super.getRange(minKey, maxKey, reverse);
        } catch (SpannerException e) {
            this.rollback();
            throw this.wrapException(e);
        }
    }

    @Override
    protected synchronized KVStore delegate() {

        // Check state
        switch (this.state) {
        case INITIAL:
            assert this.view == null;
            assert this.context == null;
            break;
        case ACCESSED:
            assert this.view != null;
            assert this.context != null;
            return this.view;
        case CLOSED:
        default:
            assert this.view == null;
            assert this.context == null;
            throw new StaleTransactionException(this);
        }

        // Create the appropriate context and view
        this.context = this.readOnly ?
          this.client.readOnlyTransaction(this.consistency) : this.readWriteTransaction();
        if (this.log.isTraceEnabled())
            this.log.trace("creating delegate: context=" + this.context);
        this.view = new ReadWriteSpannerView(this.tableName, context,
          this::wrapException, this.kvdb.getExecutorService(), INITIAL_RTT_ESTIMATE);

        // Done
        this.state = State.ACCESSED;
        return this.view;
    }

    protected RuntimeException wrapException(SpannerException e) {
        return e.isRetryable() || e instanceof AbortedException ?
          new RetryTransactionException(this, e.getMessage(), e) :
          new KVTransactionException(this, e.getMessage(), e);
    }

    private TransactionContext readWriteTransaction() {
        final TransactionRunner runner1 = this.client.readWriteTransaction();
        final TransactionRunner runner2 = (TransactionRunner)Access.read(Access.POOLED_SESSION_1_RUNNER_FIELD, runner1);
        final TransactionContext context1 = (TransactionContext)Access.read(Access.TRANSACTION_RUNNER_TXN_FIELD, runner2);
        Access.invoke(Access.TRANSACTION_CONTEXT_ENSURE_TXN_METHOD, context1);
        return context1;
    }
}

