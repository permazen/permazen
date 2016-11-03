
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.SnapshotRefs;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.util.CloseableForwardingKVStore;
import org.jsimpledb.util.ThrowableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RaftKVDatabase} transaction.
 */
@ThreadSafe
public class RaftKVTransaction implements KVTransaction {

    static final Comparator<RaftKVTransaction> SORT_BY_ID = new Comparator<RaftKVTransaction>() {
        @Override
        public int compare(RaftKVTransaction tx1, RaftKVTransaction tx2) {
            return Long.compare(tx1.txId, tx2.txId);
        }
    };

    private static final AtomicLong COUNTER = new AtomicLong();                 // provides unique transaction ID numbers

    // Package-private transaction state
    final RaftKVDatabase raft;
    final long txId = COUNTER.incrementAndGet();
    @GuardedBy("raft")
    SnapshotRefs snapshotRefs;                          // snapshot of the committed key/value store
    @GuardedBy("raft")
    long baseTerm;                                      // term of the log entry on which this transaction is based
    @GuardedBy("raft")
    long baseIndex;                                     // index of the log entry on which this transaction is based
    @GuardedBy("raft")
    KVTransactionException failure;                     // exception to throw on next access (in state EXECUTING), if any
    final MutableView view;                             // transaction's view of key/value store (restricted to prefix)
    final SettableFuture<Void> commitFuture = SettableFuture.create();
    @GuardedBy("raft")
    boolean readOnly;                                   // read-only status
    @GuardedBy("raft")
    Timer commitTimer;                                  // commit timeout timer
    @GuardedBy("raft")
    int timeout;                                        // commit timeout, or zero for none

    // Private transaction state
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    @GuardedBy("raft")
    private TxState state = TxState.EXECUTING;          // curent state
    @GuardedBy("raft")
    private Consistency consistency = Consistency.LINEARIZABLE;
    @GuardedBy("raft")
    private String[] configChange;                      // cluster config change associated with this transaction
    @GuardedBy("raft")
    private long commitTerm;                            // term of the log entry representing this transaction's commit
    @GuardedBy("raft")
    private long commitIndex;                           // index of the log entry representing this transaction's commit

    /**
     * Constructor.
     *
     * @param raft associated database
     * @param baseTerm term of the Raft log entry on which this transaction is based
     * @param baseIndex index of the Raft log entry on which this transaction is based
     * @param snapshot underlying state machine snapshot; will be closed with this instance
     * @param view this transaction's view of the (prefixed) key/value store
     */
    RaftKVTransaction(RaftKVDatabase raft, long baseTerm, long baseIndex, CloseableKVStore snapshot, MutableView view) {
        this.raft = raft;
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
        synchronized (this.raft) {
            return this.state;
        }
    }
    void setState(TxState state) {
        assert state != null;
        synchronized (this.raft) {
            assert state.compareTo(this.state) >= 0;
            if (this.state.equals(TxState.EXECUTING) && !this.state.equals(state)) {
                synchronized (this.view) {
                    this.view.setReadOnly();
                }
                this.snapshotRefs.unref();
                this.snapshotRefs = null;
            }
            this.state = state;
        }
    }

    /**
     * Get the term of the log entry on which this transaction is based.
     *
     * @return associated base log term
     */
    public long getBaseTerm() {
        synchronized (this.raft) {
            return this.baseTerm;
        }
    }

    /**
     * Get the index of the log entry on which this transaction is based.
     *
     * @return associated base log index
     */
    public long getBaseIndex() {
        synchronized (this.raft) {
            return this.baseIndex;
        }
    }

    /**
     * Get the term of the Raft log entry on which this transaction is waiting to be committed (in the Raft sense)
     * before it can complete.
     *
     * @return associated commit log entry index, or zero if this transaction has not yet gotten to {@link TxState#COMMIT_WAITING}
     */
    public long getCommitTerm() {
        synchronized (this.raft) {
            return this.commitTerm;
        }
    }
    void setCommitTerm(long commitTerm) {
        assert Thread.holdsLock(this.raft);
        this.commitTerm = commitTerm;
    }

    /**
     * Get the index of the Raft log entry on which this transaction is waiting to be committed (in the Raft sense)
     * before it can complete.
     *
     * @return associated commit log entry term, or zero if this transaction has not yet gotten to {@link TxState#COMMIT_WAITING}
     */
    public long getCommitIndex() {
        synchronized (this.raft) {
            return this.commitIndex;
        }
    }
    void setCommitIndex(long commitIndex) {
        assert Thread.holdsLock(this.raft);
        this.commitIndex = commitIndex;
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
        synchronized (this.raft) {
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
        synchronized (this.raft) {
            if (this.consistency.equals(consistency))
                return;
            Preconditions.checkState(TxState.EXECUTING.equals(this.state), "transaction is no longer open");
            this.consistency = consistency;
            this.raft.requestService(this.raft.role.rebaseTransactionsService);
        }
    }

    /**
     * Determine whether this transaction is configured as read-only.
     *
     * @return true if this transaction is configured read-only
     */
    public boolean isReadOnly() {
        synchronized (this.raft) {
            return this.readOnly;
        }
    }

    /**
     * Set whether this transaction should be read-only.
     *
     * <p>
     * Read-only transactions support modifications during the transaction, and these modifications will be visible
     * when read back, but they are discarded on {@link #commit commit()}.
     *
     * @param readOnly true to discard mutations on commit, false to apply mutations on commit
     * @throws StaleTransactionException if this transaction is no longer open
     */
    public void setReadOnly(boolean readOnly) {
        synchronized (this.raft) {
            if (readOnly == this.readOnly)
                return;
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.readOnly = readOnly;
            this.raft.requestService(this.raft.role.rebaseTransactionsService);
        }
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
        synchronized (this.raft) {
            Preconditions.checkState(this.configChange == null, "duplicate config chagne; only one is supported per transaction");
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
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
        synchronized (this.raft) {
            return this.configChange != null ? this.configChange.clone() : null;
        }
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
        }
        return this.view.get(key);
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
        }
        return this.view.getAtLeast(minKey);
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
        }
        return this.view.getAtMost(maxKey);
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
        }
        return this.view.getRange(minKey, maxKey, reverse);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
        }
        this.view.put(key, value);
    }

    @Override
    public void remove(byte[] key) {
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
        }
        this.view.remove(key);
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
        }
        this.view.removeRange(minKey, maxKey);
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
        }
        this.view.adjustCounter(key, amount);
    }

    @Override
    public byte[] encodeCounter(long value) {
        return this.view.encodeCounter(value);
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        return this.view.decodeCounter(bytes);
    }

// KVTransaction

    @Override
    public RaftKVDatabase getKVDatabase() {
        return this.raft;
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
     * @throws StaleTransactionException if this transaction is no longer open
     */
    @Override
    public void setTimeout(long timeout) {
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        synchronized (this.raft) {
            if (timeout == this.timeout)
                return;
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.timeout = (int)Math.min(timeout, Integer.MAX_VALUE);
        }
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
        return this.raft.watchKey(this, key);
    }

    @Override
    public void commit() {
        this.raft.commit(this);
    }

    @Override
    public void rollback() {
        this.raft.rollback(this);
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
        final Writes writes;
        synchronized (this.view) {
            writes = this.view.getWrites().clone();
        }
        synchronized (this.raft) {
            if (!this.state.equals(TxState.EXECUTING))
                throw new StaleTransactionException(this);
            this.throwExceptionIfAny();
            assert this.snapshotRefs != null;
            this.snapshotRefs.ref();
            final MutableView snapshotView = new MutableView(this.snapshotRefs.getKVStore(), null, writes);
            return new CloseableForwardingKVStore(snapshotView, this.snapshotRefs.getUnrefCloseable());
        }
    }

// Package-access methods

    void rebase(long baseTerm, long baseIndex, KVStore kvstore, CloseableKVStore snapshot) {
        assert Thread.holdsLock(this.view);
        assert this.state.equals(TxState.EXECUTING);
        assert this.snapshotRefs != null;
        this.rebase(baseTerm, baseIndex);
        this.view.setKVStore(kvstore);
        this.snapshotRefs.unref();
        this.snapshotRefs = new SnapshotRefs(snapshot);
    }

    void rebase(long baseTerm, long baseIndex) {
        assert Thread.holdsLock(this.raft);
        assert baseIndex > this.baseIndex;
        assert this.failure == null;
        this.baseTerm = baseTerm;
        this.baseIndex = baseIndex;
    }

    void throwExceptionIfAny() {
        assert Thread.holdsLock(this.raft);
        assert this.state.equals(TxState.EXECUTING);
        final KVTransactionException e = this.failure;
        if (e != null) {
            this.failure = null;
            this.raft.cleanupTransaction(this);
            ThrowableUtil.prependCurrentStackTrace(e);
            throw e;
        }
    }

    /**
     * Determine if committing this transaction will require appending a new log entry to the Raft log.
     *
     * @return true if this is not a read-only transaction and has either a config change or a key/value store mutation
     */
    boolean addsLogEntry() {
        assert Thread.holdsLock(this.raft);
        if (this.readOnly)
            return false;
        if (this.configChange != null)
            return true;
        synchronized (this.view) {
            return !this.view.getWrites().isEmpty();
        }
    }

// Object

    @Override
    public String toString() {
        synchronized (this.raft) {
            return this.getClass().getSimpleName()
              + "[txId=" + this.txId
              + ",state=" + this.state
              + ",base=" + this.baseIndex + "t" + this.baseTerm
              + ",consistency=" + this.consistency
              + (this.readOnly ? ",readOnly" : "")
              + (this.configChange != null ? ",configChange=" + this.configChange[0] + "@" + this.configChange[1] : "")
              + (this.state.compareTo(TxState.COMMIT_WAITING) >= 0 ? ",commit=" + this.commitIndex + "t" + this.commitTerm : "")
              + (this.timeout != 0 ? ",timeout=" + this.timeout : "")
              + "]";
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            synchronized (this.raft) {
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
            assert this.commitTerm == 0;
            assert this.commitIndex == 0;
            assert this.snapshotRefs != null;
            break;
        case COMMIT_READY:
            assert this.commitTerm == 0;
            assert this.commitIndex == 0;
            assert this.snapshotRefs == null;
            assert this.failure == null;
            break;
        case COMMIT_WAITING:
            assert this.commitTerm >= this.baseTerm;
            assert this.commitTerm <= currentTerm;
            assert this.commitIndex >= this.baseIndex;                                      // equal implies a read-only tx
            assert this.commitIndex > this.baseIndex || !this.addsLogEntry();
            assert this.failure == null;
            break;
        case COMPLETED:
            assert this.commitFuture.isDone();
            assert this.commitTerm == 0 || this.commitTerm >= this.baseTerm;
            assert this.commitIndex == 0 || this.commitIndex >= this.baseIndex;
            assert this.commitTerm <= currentTerm;
            assert this.failure == null;
            break;
        case CLOSED:
        default:
            assert false;
            break;
        }
    }
}

