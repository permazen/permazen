
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionManager;
import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SpannerKVDatabase} transaction.
 */
@ThreadSafe
public class SpannerKVTransaction extends ForwardingKVStore implements KVTransaction {

    // Lock order: (1) SpannerKVTransaction, (2) SpannerKVDatabase

    private enum State {
        INITIAL,                    // transaction is open, but no data has been accessed yet
        ACCESSED,                   // transaction is open, and some data has been queried from spanner
        CLOSED                      // transaction is closed
    };

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
    private TransactionManager transactionManager;              // read/write transactions only
    @GuardedBy("this")
    private ReadWriteSpannerView view;
    @GuardedBy("this")
    private State state = State.INITIAL;

    // Workaround for https://github.com/GoogleCloudPlatform/google-cloud-java/issues/3172
    @GuardedBy("this")
    private boolean transactionManagerClosed;

    /**
     * Constructor.
     *
     * @param kvdb associated database
     * @param client client for access
     * @param tableName Spanner key/value database table name
     * @param consistency transaction consistency level
     * @throws IllegalArgumentException if any paramter is null
     */
    @SuppressWarnings("this-escape")
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
        if (this.log.isTraceEnabled())
            this.log.trace("{}: created from client={}", this, this.client);
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
     * For read-write transactions, this returns the Spanner timestamp at which the changes were successfully applied.
     *
     * @return this transaction's Spanner timestamp
     * @throws IllegalStateException if timestamp is not available yet
     * @throws IllegalStateException if invoked on a read/write transaction that was not successfully committed
     */
    public synchronized Timestamp getTimestamp() {
        try {
            switch (this.state) {
            case INITIAL:
                throw new IllegalStateException("no data has been accessed yet");
            case ACCESSED:
                if (!this.readOnly)
                    throw new IllegalStateException("transaction is not committed yet");
                break;
            case CLOSED:
            default:
                break;
            }
            if (this.context instanceof TransactionContext)
                return this.transactionManager.getCommitTimestamp();
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
        if (this.log.isTraceEnabled())
            this.log.trace("{}: setting readOnly={}", this, readOnly);
        this.readOnly = readOnly;
    }

    @Override
    public synchronized void commit() {

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("{}: commit() invoked: state={} view={} context={} txmgr={}[{}]",
              this, this.state, this.view, this.context, this.transactionManager,
              this.transactionManager != null ? this.transactionManager.getState() : "");
        }

        // Check state
        switch (this.state) {
        case INITIAL:
            assert this.view == null;
            assert this.context == null;
            assert this.transactionManager == null;
            this.state = State.CLOSED;
            return;
        case ACCESSED:
            break;
        case CLOSED:
        default:
            throw new StaleKVTransactionException(this);
        }

        // Commit transaction (if read/write) and close view
        try {
            if (this.context instanceof TransactionContext) {
                assert this.transactionManager != null;

                // Transfer mutations into the transaction context
                this.view.bufferMutations((TransactionContext)this.context);

                // Commit transaction
                if (this.log.isTraceEnabled()) {
                    this.log.trace("{}: committing context={} txmgr={}[{}]",
                      this, this.context, this.transactionManager, this.transactionManager.getState());
                }
                try {
                    this.transactionManager.commit();
                } finally {
                    if (this.transactionManager.getState() != TransactionManager.TransactionState.ABORTED)
                        this.transactionManagerClosed = true;
                }
                if (this.log.isTraceEnabled())
                    this.log.trace("{}: commit successful", this);
            }
        } catch (SpannerException e) {
            if (this.log.isTraceEnabled())
                this.log.trace("{}: commit failed: ", this, e.toString());
            throw this.wrapException(e);
        } finally {
            this.cleanup();
        }
    }

    @Override
    public synchronized void rollback() {

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("{}: rollback() invoked: state={} view={} context={} txmgr={}[{}]",
              this, this.state, this.view, this.context, this.transactionManager,
              this.transactionManager != null ? this.transactionManager.getState() : "");
        }

        // Check state
        switch (this.state) {
        case INITIAL:
            assert this.view == null;
            assert this.context == null;
            assert this.transactionManager == null;
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
            if (this.context instanceof TransactionContext) {
                assert this.transactionManager != null;
                if (this.log.isTraceEnabled()) {
                    this.log.trace("{}: rolling back context={} txmgr={}[{}]",
                      this, this.context, this.transactionManager, this.transactionManager.getState());
                }
                this.transactionManagerClosed = true;
                this.transactionManager.rollback();
                if (this.log.isTraceEnabled())
                    this.log.trace("{}: rollback successful", this);
            }
        } catch (SpannerException e) {
            if (this.log.isDebugEnabled())
                this.log.debug(this + ": got exception during rollback (ignoring)", e);
        } finally {
            this.cleanup();
        }
    }

    private void cleanup() {

        // Sanity check
        assert Thread.holdsLock(this);
        assert State.ACCESSED.equals(this.state);

        // Update database RTT estimate
        this.kvdb.updateRttEstimate(this.view.getRttEstimate());

        // Close the view
        try {
            if (this.log.isTraceEnabled()) {
                this.log.trace("{}: cleanup(): view={} context={} txmgr={}[{}]: closing view",
                  this, this.view, this.context, this.transactionManager,
                  this.transactionManager != null ? this.transactionManager.getState() : "");
            }
            this.view.close();
            if (this.log.isTraceEnabled())
                this.log.trace("{}: cleanup(): view closed", this);
        } finally {
            this.view = null;
            this.context = null;
            this.state = State.CLOSED;
        }

        // Close the transaction manager, if any
        if (this.transactionManager != null && !this.transactionManagerClosed) {
            if (this.log.isTraceEnabled())
                this.log.trace("{}: cleanup(): closing txmgr", this);
            this.transactionManager.close();
            this.transactionManagerClosed = true;
            if (this.log.isTraceEnabled())
                this.log.trace("{}: cleanup(): txmgr closed", this);
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
    public Future<Void> watchKey(ByteData key) {
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
     * {@link SpannerKVDatabase#snapshot SpannerKVDatabase.snapshot()} and {@link MutableView}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public CloseableKVStore readOnlySnapshot() {
        throw new UnsupportedOperationException();
    }

// ForwardingKVStore

    @Override
    public ByteData get(ByteData key) {
        try {
            return super.get(key);
        } catch (SpannerException e) {
            this.rollback();
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        try {
            return super.getAtLeast(minKey, maxKey);
        } catch (SpannerException e) {
            this.rollback();
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        try {
            return super.getAtMost(maxKey, minKey);
        } catch (SpannerException e) {
            this.rollback();
            throw this.wrapException(e);
        }
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
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
            assert this.transactionManager == null;
            break;
        case ACCESSED:
            assert this.view != null;
            assert this.context != null;
            assert this.transactionManager != null == !this.readOnly;
            if (this.log.isTraceEnabled()) {
                this.log.trace("{}: delegate(): view={} exists, context={}, txmgr={}[{}]",
                  this, this.view, this.context, this.transactionManager,
                  this.transactionManager != null ? this.transactionManager.getState() : "");
            }
            return this.view;
        case CLOSED:
        default:
            assert this.view == null;
            assert this.context == null;
            throw new StaleKVTransactionException(this);
        }

        // Create the appropriate context and view
        if (this.readOnly) {
            if (this.log.isTraceEnabled())
                this.log.trace("{}: delegate(): creating r/o transaction", this);
            this.context = this.client.readOnlyTransaction(this.consistency);
            if (this.log.isTraceEnabled())
                this.log.trace("{}: delegate(): created r/o transaction, context={}", this, this.context);
        } else {
            if (this.log.isTraceEnabled())
                this.log.trace("{}: delegate(): creating r/w transaction", this);
            this.transactionManager = this.client.transactionManager();
            this.context = this.transactionManager.begin();
            if (this.log.isTraceEnabled()) {
                this.log.trace("{}: delegate(): created r/w transaction, context={}, txmgr={}[{}]",
                  this, this.context, this.transactionManager, this.transactionManager.getState());
            }
        }
        this.view = new ReadWriteSpannerView(this.tableName, context,
          this::wrapException, this.kvdb.getExecutorService(), (long)this.kvdb.getRttEstimate());
        if (this.log.isTraceEnabled())
            this.log.trace("{}: delegate(): created view={}", this, this.view);

        // Done
        this.state = State.ACCESSED;
        return this.view;
    }

    protected RuntimeException wrapException(SpannerException e) {
        return e.isRetryable() || e instanceof AbortedException || ErrorCode.ABORTED.equals(e.getErrorCode()) ?
          new RetryKVTransactionException(this, e.getMessage(), e) :
          new KVTransactionException(this, e.getMessage(), e);
    }
}
