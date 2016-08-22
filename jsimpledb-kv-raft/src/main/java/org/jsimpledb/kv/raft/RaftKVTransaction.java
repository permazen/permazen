
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.SnapshotRefs;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.util.CloseableForwardingKVStore;
import org.jsimpledb.kv.util.ForwardingKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RaftKVDatabase} transaction.
 */
@ThreadSafe
public class RaftKVTransaction extends ForwardingKVStore implements KVTransaction {

    static final Comparator<RaftKVTransaction> SORT_BY_ID = new Comparator<RaftKVTransaction>() {
        @Override
        public int compare(RaftKVTransaction tx1, RaftKVTransaction tx2) {
            return Long.compare(tx1.getTxId(), tx2.getTxId());
        }
    };

    private static final AtomicLong COUNTER = new AtomicLong();                 // provides unique transaction ID numbers

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    // Transaction info
    private final long txId = COUNTER.incrementAndGet();
    private final SettableFuture<Void> commitFuture = SettableFuture.create();
    private final RaftKVDatabase kvdb;
    private final SnapshotRefs snapshotRefs;            // snapshot of the committed key/value store
    private final MutableView view;                     // transaction's view of key/value store (restricted to prefix)
    private final long baseTerm;                        // term of the log entry on which this transaction is based
    private final long baseIndex;                       // index of the log entry on which this transaction is based

    @GuardedBy("kvdb")
    private TxState state = TxState.EXECUTING;
    @GuardedBy("kvdb")
    private Consistency consistency = Consistency.LINEARIZABLE;
    private volatile boolean readOnly;
    private volatile int timeout;
    private volatile String[] configChange;             // cluster config change associated with this transaction
    @GuardedBy("kvdb")
    private Timer commitTimer;
    @GuardedBy("kvdb")
    private long commitTerm;                            // term of the log entry representing this transaction's commit
    @GuardedBy("kvdb")
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
        this.snapshotRefs = new SnapshotRefs(snapshot);
        this.view = view;
    }

// Properties

    /**
     * Get the locally unique ID of this transaction.
     *
     * @return transaction ID
     */
    public long getTxId() {
        return this.txId;
    }

    /**
     * Get the state of this transaction.
     *
     * @return transaction state
     */
    public TxState getState() {
        synchronized (this.kvdb) {
            return this.state;
        }
    }
    void setState(TxState state) {
        assert state != null;
        synchronized (this.kvdb) {
            assert state.compareTo(this.state) >= 0;
            if (this.state.equals(TxState.EXECUTING) && !this.state.equals(state))
                this.snapshotRefs.unref();
            this.state = state;
        }
    }

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
     * Get the term of the Raft log entry on which this transaction is waiting to be committed (in the Raft sense)
     * before it can complete.
     *
     * @return associated commit log entry index, or zero if this transaction has not yet gotten to {@link TxState#COMMIT_WAITING}
     */
    public long getCommitTerm() {
        synchronized (this.kvdb) {
            return this.commitTerm;
        }
    }
    void setCommitTerm(long commitTerm) {
        synchronized (this.kvdb) {
            this.commitTerm = commitTerm;
        }
    }

    /**
     * Get the index of the Raft log entry on which this transaction is waiting to be committed (in the Raft sense)
     * before it can complete.
     *
     * @return associated commit log entry term, or zero if this transaction has not yet gotten to {@link TxState#COMMIT_WAITING}
     */
    public long getCommitIndex() {
        synchronized (this.kvdb) {
            return this.commitIndex;
        }
    }
    void setCommitIndex(long commitIndex) {
        synchronized (this.kvdb) {
            this.commitIndex = commitIndex;
        }
    }

    /**
     * Get the consistency level for this transaction.
     *
     * <p>
     * The default consistency level is {@link Consistency#LINEARIZABLE}.
     *
     * @return transaction consistency level
     */
    public Consistency getConsistency() {
        synchronized (this.kvdb) {
            return this.consistency;
        }
    }

    /**
     * Set the consistency level for this transaction.
     *
     * <p>
     * This setting may be modified freely during a transaction while it is still open;
     * it only determines the behavior of the transaction after {@link #commit} is invoked.
     *
     * @param consistency desired consistency level
     * @see <a href="https://aphyr.com/posts/313-strong-consistency-models">Strong consistency models</a>
     * @throws IllegalStateException if {@code consistency} is different from the {@linkplain #getConsistency currently configured
     *  consistency} but this transaction is no longer open (i.e., in state {@link TxState#EXECUTING})
     * @throws IllegalArgumentException if {@code consistency} is null
     */
    public void setConsistency(Consistency consistency) {
        Preconditions.checkArgument(consistency != null, "null consistency");
        synchronized (this.kvdb) {
            if (this.consistency.equals(consistency))
                return;
            Preconditions.checkState(TxState.EXECUTING.equals(this.state), "transaction is no longer open");
            this.consistency = consistency;
        }
    }

    /**
     * Determine whether this transaction is configured as read-only.
     *
     * @return true if this transaction is configured read-only
     */
    public boolean isReadOnly() {
        return this.readOnly;
    }

    /**
     * Set whether this transaction should be read-only.
     *
     * <p>
     * Read-only transactions support modifications during the transaction, and these modifications will be visible
     * when read back, but they are discarded on {@link #commit commit()}.
     *
     * @param readOnly true to discard mutations on commit, false to apply mutations on commit
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

// Configuration Stuff

    /**
     * Include a cluster configuration change when this transaction is committed.
     *
     * <p>
     * The change will have been applied once this transaction is successfully committed.
     *
     * <p>
     * Raft supports configuration changes that add or remove one node at a time to/from the cluster.
     * If this method is invoked more than once in a single transaction, all but the last invocation are ignored.
     *
     * <p>
     * Initially, nodes are <i>unconfigured</i>. An unconfigured node becomes configured in one of two ways:
     * <ul>
     *  <li>By receiving a message from a leader of some existing cluster, in which case the node joins that cluster
     *      based on the provided configuration; or</li>
     *  <li>By this method being invoked with {@code identity} equal to this node's identity and a non-null {@code address},
     *      which creates a new cluster and adds this node to it.</li>
     * </ul>
     *
     * <p>
     * Therefore, this method must be used to intialize a new cluster.
     *
     * @param identity the identity of the node to add or remove
     * @param address the network address of the node if adding, or null if removing
     * @throws IllegalStateException if this method has been invoked previously on this instance
     * @throws IllegalArgumentException if {@code identity} is null
     */
    public void configChange(String identity, String address) {
        Preconditions.checkArgument(identity != null, "null identity");
        synchronized (this.kvdb) {
            Preconditions.checkState(this.configChange == null, "duplicate config chagne; only one is supported per transaction");
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.configChange = new String[] { identity, address };
        }
    }

    /**
     * Get the cluster configuration change associated with this transaction, if any.
     *
     * <p>
     * The returned array has length two and contains the {@code identity} and {@code address}
     * parameters passed to {@link #configChange configChange()}.
     *
     * <p>
     * The returned array is a copy; changes have no effect on this instance.
     *
     * @return cluster config change, or null if there is none
     */
    public String[] getConfigChange() {
        synchronized (this.kvdb) {
            return this.configChange != null ? this.configChange.clone() : null;
        }
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
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        this.timeout = (int)Math.min(timeout, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Key watches are supported by {@link RaftKVTransaction}.
     *
     * <p>
     * Raft key watches are compatible with all {@link Consistency} levels, in that if a key watch fires due
     * to a mutation to some key, then a subsequent transaction will see that mutation, no matter what
     * {@link Consistency} level is configured for that transaction.
     *
     * <p>
     * Listeners registered on the returned {@link ListenableFuture} must not perform any long running
     * or blocking operations.
     *
     * @param key {@inheritDoc}
     * @return {@inheritDoc}
     * @throws StaleTransactionException {@inheritDoc}
     * @throws org.jsimpledb.kv.RetryTransactionException {@inheritDoc}
     * @throws org.jsimpledb.kv.KVDatabaseException {@inheritDoc}
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public ListenableFuture<Void> watchKey(byte[] key) {
        return this.kvdb.watchKey(this, key);
    }

    @Override
    public void commit() {
        this.kvdb.commit(this);
    }

    @Override
    public void rollback() {
        this.kvdb.rollback(this);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Mutable snapshots are supported by {@link RaftKVTransaction}.
     *
     * @return {@inheritDoc}
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws StaleTransactionException {@inheritDoc}
     * @throws org.jsimpledb.kv.RetryTransactionException {@inheritDoc}
     */
    @Override
    public CloseableKVStore mutableSnapshot() {
        synchronized (this.kvdb) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.snapshotRefs.ref();
        }
        final Writes writes;
        synchronized (this.view) {
            writes = this.view.getWrites().clone();
        }
        final MutableView snapshotView = new MutableView(this.snapshotRefs.getKVStore(), null, writes);
        return new CloseableForwardingKVStore(snapshotView, this.snapshotRefs.getUnrefCloseable());
    }

// Package-access methods

    int getTimeout() {
        return this.timeout;
    }

    SettableFuture<Void> getCommitFuture() {
        return this.commitFuture;
    }

    MutableView getMutableView() {
        return this.view;
    }

    Timer getCommitTimer() {
        return this.commitTimer;
    }
    void setCommitTimer(Timer commitTimer) {
        this.commitTimer = commitTimer;
    }

// Object

    @Override
    public String toString() {
        synchronized (this.kvdb) {
            return this.getClass().getSimpleName()
              + "[txId=" + this.txId
              + ",state=" + this.state
              + ",base=" + this.baseIndex + "t" + this.baseTerm
              + ",consistency=" + this.consistency
              + (this.readOnly ? ",readOnly" : "")
              + (this.configChange != null ? ",configChange=" + Arrays.<String>asList(this.configChange) : "")
              + (this.state.compareTo(TxState.COMMIT_WAITING) >= 0 ? ",commit=" + this.commitIndex + "t" + this.commitTerm : "")
              + (this.timeout != 0 ? ",timeout=" + this.timeout : "")
              + "]";
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            synchronized (this.kvdb) {
                if (!TxState.CLOSED.equals(this.state)) {
                    this.log.warn(this + " leaked without commit() or rollback()");
                    this.rollback();
                }
            }
        } finally {
            super.finalize();
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

