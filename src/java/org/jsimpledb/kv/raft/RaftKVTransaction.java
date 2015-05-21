
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.util.ForwardingKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RaftKVDatabase} transaction.
 */
public class RaftKVTransaction extends ForwardingKVStore implements KVTransaction, Closeable {

    private static final AtomicLong COUNTER = new AtomicLong();                 // provides unique transaction ID numbers

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    // Transaction info
    private final long txId = COUNTER.incrementAndGet();
    private final CommitFuture commitFuture = new CommitFuture();
    private final RaftKVDatabase kvdb;
    private CloseableKVStore snapshot;                  // snapshot of the committed key/value store
    private final MutableView view;                     // transaction's view of key/value store (restricted to prefix)
    private final long baseTerm;                        // term of the log entry on which this transaction is based
    private final long baseIndex;                       // index of the log entry on which this transaction is based

    private volatile TxState state = TxState.EXECUTING;
    private volatile boolean readOnlySnapshot;
    private volatile int timeout;
    private RaftKVDatabase.Timer commitTimer;
    private long commitTerm;                            // term of the log entry representing this transaction's commit
    private long commitIndex;                           // index of the log entry representing this transaction's commit

    /**
     * Constructor.
     *
     * @param kvdb associated database
     * @param baseTerm term of the Raft log entry on which this transaction is based
     * @param baseIndex index of the Raft log entry on which this transaction is based
     * @param snapshot underlying state machine snapshot; will be closed with this instance
     * @param view this transaction's view of the (prefixed) key/value store
     */
    RaftKVTransaction(RaftKVDatabase kvdb, long baseTerm, long baseIndex, CloseableKVStore snapshot, MutableView view) {
        this.kvdb = kvdb;
        this.baseTerm = baseTerm;
        this.baseIndex = baseIndex;
        this.snapshot = snapshot;
        this.view = view;
    }

// Properties

    /**
     * Get the term of the log entry on which this transaction is based.
     *
     * @return associated base log term
     */
    public long getBaseTerm() {
        return this.baseTerm;
    }

    /**
     * Get the index of the log entry on which this transaction is based.
     *
     * @return associated base log index
     */
    public long getBaseIndex() {
        return this.baseIndex;
    }

    /**
     * Configure whether to reduce the isolation guarantees for this transaction from linearizable consistency
     * to read-only, "stale snapshot" consistency (technically, serializable consistency).
     *
     * <p>
     * If so configured, this transaction guarantees a consistent, read-only view of the database as it existed
     * at some point in the "recent past". The view is not guaranteed to be up-to-date; it's only guaranteed
     * to be as up-to-date as is known to this node when the transaction was opened. For example, it will be
     * at least as up to date as the most recently committed normal (linearizable) transaction on this node.
     * However, in general (for example, if stuck in a network partition minority), the snapshot could be arbitrarily
     * far in the past.
     *
     * <p>
     * This setting may be modified freely during a transaction; it only determines behavior at {@link #commit} time.
     * If set:
     *  <ul>
     *  <li>The transaction will be read-only: modifications will be allowed (and reflected in subsequent reads),
     *      but they will be discarded on {@link #commit}.</li>
     *  <li>The {@link #commit} operation will be faster, and possibly not require any network traffic at all.</li>
     *  </ul>
     *
     * @param readOnlySnapshot true for snapshot isolation, false for normal linearizable ACID semantics
     * @see <a href="https://aphyr.com/posts/313-strong-consistency-models">Strong consistency models</a>
     */
    public void setReadOnlySnapshot(boolean readOnlySnapshot) {
        this.readOnlySnapshot = readOnlySnapshot;
    }

    /**
     * Determine whether this instance is configured for read-only, snapshot isolation.
     *
     * @return true if this instance is configured for snapshot isolation
     * @see #setReadOnlySnapshot
     */
    public boolean isReadOnlySnapshot() {
        return this.readOnlySnapshot;
    }

// ForwardingKVStore

    @Override
    protected KVStore delegate() {
        if (!this.state.equals(TxState.EXECUTING))
            throw new StaleTransactionException(this);
        return this.view;
    }

// KVTransaction

    @Override
    public RaftKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    /**
     * Set the commit timeout for this instance.
     *
     * <p>
     * {@link RaftKVTransaction}s do not block while the transaction is open; the configured value is used
     * as a timeout for the {@link #commit} operation only. If {@link #commit} takes longer than {@code timeout}
     * milliseconds, a {@link org.jsimpledb.kv.RetryTransactionException} is thrown.
     *
     * <p>
     * The default value for all transactions is configured by {@link RaftKVDatabase#setCommitTimeout}.
     *
     * @param timeout transaction commit timeout in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    @Override
    public void setTimeout(long timeout) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout < 0");
        this.timeout = (int)Math.min(timeout, Integer.MAX_VALUE);
    }

    @Override
    public void commit() {
        this.kvdb.commit(this);
    }

    @Override
    public void rollback() {
        this.kvdb.rollback(this);
    }

// Package-access methods

    long getTxId() {
        return this.txId;
    }

    int getTimeout() {
        return this.timeout;
    }

    CommitFuture getCommitFuture() {
        return this.commitFuture;
    }

    MutableView getMutableView() {
        return this.view;
    }

    TxState getState() {
        return this.state;
    }
    void setState(TxState state) {
        this.state = state;
    }

    RaftKVDatabase.Timer getCommitTimer() {
        return this.commitTimer;
    }
    void setCommitTimer(RaftKVDatabase.Timer commitTimer) {
        this.commitTimer = commitTimer;
    }

    long getCommitTerm() {
        return this.commitTerm;
    }
    void setCommitTerm(long commitTerm) {
        this.commitTerm = commitTerm;
    }

    long getCommitIndex() {
        return this.commitIndex;
    }
    void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    void closeSnapshot() {
        if (this.snapshot != null) {
            this.snapshot.close();
            this.snapshot = null;
        }
    }

// Closeable

    @Override
    public void close() {
        this.rollback();
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[txId=" + this.txId
          + ",state=" + this.state
          + ",base=" + this.baseTerm + "/" + this.baseIndex
          + (this.state.compareTo(TxState.COMMIT_WAITING) >= 0 ? ",commit=" + this.commitTerm + "/" + this.commitIndex : "")
          + (this.timeout != 0 ? ",timeout=" + this.timeout : "")
          + "]";
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (!TxState.CLOSED.equals(this.state))
               this.log.warn(this + " leaked without commit() or rollback()");
            this.close();
        } finally {
            super.finalize();
        }
    }

// CommitFuture

    static class CommitFuture extends FutureTask<Void> {

        private final ExceptionCallable callable;

        public CommitFuture() {
            this(new ExceptionCallable());
        }

        CommitFuture(ExceptionCallable callable) {
            super(callable);
            this.callable = callable;
        }

        public void succeed() {
            Preconditions.checkState(!this.isDone(), "already done");
            this.run();
        }

        public void fail(Exception e) {
            Preconditions.checkState(!this.isDone(), "already done");
            this.callable.setException(e);
            this.run();
        }
    }

    private static class ExceptionCallable implements Callable<Void> {

        private volatile Exception e;

        public void setException(Exception e) {
            this.e = e;
        }

        @Override
        public Void call() throws Exception {
            if (this.e != null)
                throw this.e;
            return null;
        }
    }

// Debug/Sanity Checking

    void checkStateOpen(long currentTerm, long lastIndex, long raftCommitIndex) {
        assert !this.commitFuture.isCancelled();
        assert this.commitFuture.isDone() == this.state.compareTo(TxState.COMPLETED) >= 0;
        assert this.baseTerm <= currentTerm;
        switch (this.state) {
        case EXECUTING:
        case COMMIT_READY:
            assert this.commitTerm == 0;
            assert this.commitIndex == 0;
            break;
        case COMMIT_WAITING:
            assert this.commitTerm >= this.baseTerm;
            assert this.commitTerm <= currentTerm;
            assert this.commitIndex >= this.baseIndex;                                      // equal implies a read-only tx
            assert this.commitIndex > this.baseIndex || this.view.getWrites().isEmpty();
            break;
        case COMPLETED:
            assert this.commitFuture.isDone();
            assert this.commitTerm == 0 || this.commitTerm >= this.baseTerm;
            assert this.commitIndex == 0 || this.commitIndex >= this.baseIndex;
            assert this.commitTerm <= currentTerm;
            break;
        case CLOSED:
        default:
            assert false;
            break;
        }
    }
}

