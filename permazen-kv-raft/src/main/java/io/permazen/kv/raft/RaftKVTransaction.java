
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.mvcc.Writes;
import io.permazen.kv.util.CloseableForwardingKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;
import io.permazen.util.CloseableRefs;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RaftKVDatabase} transaction.
 */
@ThreadSafe
public class RaftKVTransaction implements KVTransaction {

/*

   How transactions are managed
   ============================

   Transactions have a base term+index, which dictates the MVCC version of the database that the transaction sees,
   and (eventually) a commit term+index, which represents the log entry that must be committed (in the Raft sense)
   before the transaction itself may commit.

   When a transaction is created, a MutableView is setup using the log entry corresponding to the transaction's base
   term+index as the underlying read-only data. The transaction's consistency determines whether this log entry is
   the last log entry (LINEARIZABLE, EVENTUAL, UNCOMMITTED) or the last committed log entry (EVENTUAL_COMMITTED).

   For non-LINEARIZABLE transactions, the commit term+index can always be determined immediately: for EVENTUAL and
   EVENTUAL_COMMITTED, it is just the base log entry; for UNCOMMITTED, both values are zero. Therefore, UNCOMMITTED
   and EVENTUAL_COMMITTED transactions can always commit() immediately, because their commit term+index is already
   committed at the time the transaction starts.

   For LINEARIZABLE transactions, the base term+index must change over time as new log entries are received, to keep
   the transaction's view up-to-date. This is called "rebasing" the transaction. The rebase operation can fail due
   to read/write conflicts, whereby a newly added log entry mutates a key that has already been read by the transaction.
   If such a conflict is detected, the transaction must retry because it has seen what is now out-of-date information.
   On leaders, if a conflict occurs when rebasing a high priority transaction, the other transaction fails instead.

   For LINEARIZABLE transactions, the leader must be consulted (via CommitRequest) to determine the commit term+index.
   If the transaction is read-only, the commit term+index is taken from the leader's last log entry at the time the
   CommitRequest is received. In addition, the transaction must also wait until the leader's lease expiration time
   exceeds the leader's current time, to guarantee that the leader's last log entry is in fact up-to-date (i.e., there's
   not some other leader we don't know about who has already been elected).

   If the LINEARIZABLE transaction is read-write, the leader checks for conflicts caused by any log entries it has between
   the transaction's base log entry and the leader's last log entry, then appends a new log entry, and returns the new log
   entry as the transaction's commit term+index in a CommitResponse. Followers do not need to rebase beyond the base
   term+index that was sent to the leader in the CommitRequest, because the leader takes over conflict detection from that
   point as just described.

   When a follower transaction is set read-only, the follower requests a commit term+index from the leader immediately
   (linearizability only guarantees transactions see the database state as it existed at some time between begin and
   commit, and for the purposes of minimizing conflicts, the sooner we take that "snapshot" the better). In any case,
   followers must continue to rebase read-only LINEARIZABLE transactions until the commit term+index is received.

   If a follower transaction's base log entry is ever overwritten in the Raft log, then the transaction fails immediately
   with a retry (unless UNCOMMITTED). This can occur both during normal execution, and while blocked in commit(). Therefore
   it's always the case that a transaction's base term+index actually exists in the node's Raft log, possibly compacted
   (unless UNCOMMITTED).

   CommitRequest's
   ---------------

   CommitRequest's are sent by followers to leaders for LINEARIZABLE transactions. For read-write transactions, they are
   sent when commit() is invoked, and contain both the transaction's reads and writes. For read-only transactions, the
   CommitRequest is sent as soon as possible, i.e., as soon as it is known that the transaction is read-only. This is
   allowed because the "up-to-date" guarantee of LINEARIZABLE is only that the transaction sees the database as it
   existed at some point in time between begin() and commit().

   When commit() is invoked, the thread blocks until the commit term+index log entry is committed. The leader's response
   may be delayed or lost; if so the transaction times out.

   Compacting Log Entries
   ----------------------

   A transaction's base log entry, if committed, may be applied into the state machine without affecting the transaction
   due to the view being based on a read-only AtomicKVStore snapshot, which persists until closed.

   Therefore because LINEARIZABLE transactions are always rebased to the last log entry (or their commit log entry, at which
   point rebasing stops), and all other transactions are never rebased, there is no reason for followers to not apply committed
   log entries immediately.

   For leaders, the situation is more complicated. Applying committed log entries too aggressively can cause these issues:

    o If some follower has not received a log entry, but the leader has applied that log entry to its state machine and
      discarded it, then the only way the follower can be synchronized is via InstallSnapshot (i.e., full state machine dump),
      which is costly.

    o In order to detect conflicts in a mutating LINEARIZABLE transaction received in a follower's CommitRequest, a leader
      must have access to the mutations associated with all log entries after the transaction's base term+index. If any of
      these have already been applied to the state machine and discarded, the leader has no choice but to return a retry error.

   Actually, these issues also apply to followers, in the sense that they could become leaders at any time.

   To address these issues, leaders remember up to Log.MAX_APPLIED already-applied log entries. However, once it is known
   that all followers have received an already-applied log entry, then it can be discarded. This is because followers keep all
   transactions rebased to their last log entry, and so, under the assumption that message re-ordering is rare or impossible,
   any CommitRequest a leader receives will have a base index >= that follower's last log entry index.

   To save memory, we discard the Writes object (which is an in-memory representation of the entry's mutations) associated
   with already-applied log entries. This is just an optimization: conflict detection is still possible by re-reading the
   mutations from disk (see LogEntry.getMutations()), which should only be needed if a follower gets so far behind that its
   last log entry index is less than the leader's commit index (because followers always rebase to their last log entry).

   Lock Order
   ----------

   If locks are to be obtained on both this.raft and this.view, the order is: (1) this.raft, (2) this.view

*/

    static final Comparator<RaftKVTransaction> SORT_BY_ID = Comparator.comparingLong(tx -> tx.txId);

    private static final AtomicLong COUNTER = new AtomicLong();                 // provides unique transaction ID numbers

    // Static setup
    final RaftKVDatabase raft;
    final long txId = COUNTER.incrementAndGet();                // transaction's unique ID (on this node)
    final Consistency consistency;                              // transaction's consistency level guarantee
    final MutableView view;                                     // transaction's view of key/value store

    // Base and commit log entry
    @GuardedBy("raft")
    private long baseTerm;                                      // term of the log entry on which this transaction is based
    @GuardedBy("raft")
    private long baseIndex;                                     // index of the log entry on which this transaction is based
    @GuardedBy("raft")
    private long commitTerm;                                    // term of the log entry representing this transaction's commit
    @GuardedBy("raft")
    private long commitIndex;                                   // index of the log entry representing this transaction's commit
    @GuardedBy("raft")
    private boolean rebasable;                                  // transaction should be rebased on newly committed log entries

    // Transaction state
    @GuardedBy("raft")
    private TxState state = TxState.EXECUTING;                  // curent state
    private volatile boolean executing = true;                  // allows for quick checks without synchronization
    @GuardedBy("raft")
    private Timestamp lastStateChangeTime = new Timestamp();    // timestamp of most recent state change
    @GuardedBy("raft")
    private boolean readOnly;                                   // read-only status
    @GuardedBy("raft")
    private String[] configChange;                              // cluster config change associated with this transaction
    @GuardedBy("raft")
    private KVTransactionException failure;                     // exception to throw on next access, if any; state CLOSED only
    @GuardedBy("raft")
    private CloseableRefs<CloseableKVStore> snapshotRefs;       // snapshot of the committed key/value store

    // commit() status
    private final SettableFuture<Void> commitFuture = SettableFuture.create();  // the result for the thread invoking commit()
    @GuardedBy("raft")
    private Timer commitTimer;                                  // commit timeout timer
    @GuardedBy("raft")
    private int timeout;                                        // commit timeout, or zero for none
    @GuardedBy("raft")
    private boolean committable;                                // transaction can be committed after commitLeaderLeaseTimeout
    @GuardedBy("raft")
    private Timestamp commitLeaderLeaseTimeout;                 // minimum required leader lease timeout for commit (if not null)

    // Private transaction state
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param raft associated database
     * @param consistency consistency guarantee
     * @param baseTerm term of the Raft log entry on which this transaction is based
     * @param baseIndex index of the Raft log entry on which this transaction is based
     * @param snapshot underlying state machine snapshot; will be closed with this instance
     * @param view this transaction's view of the (prefixed) key/value store
     */
    RaftKVTransaction(RaftKVDatabase raft, Consistency consistency,
      long baseTerm, long baseIndex, CloseableKVStore snapshot, MutableView view) {
        this.raft = raft;
        this.consistency = consistency;
        this.baseTerm = baseTerm;
        this.baseIndex = baseIndex;
        this.snapshotRefs = new CloseableRefs<>(snapshot);
        this.view = view;
        this.rebasable = consistency.isGuaranteesUpToDateReads();                   // i.e., LINEARIZABLE
        if (!this.rebasable)
            this.view.disableReadTracking();
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
    void setState(final TxState state) {
        assert state != null;
        synchronized (this.raft) {
            assert state.compareTo(this.state) >= 0;
            if (this.state.equals(TxState.EXECUTING) && !this.state.equals(state)) {
                this.view.setReadOnly();
                this.snapshotRefs.unref();
                this.snapshotRefs = null;
            }
            this.state = state;
            this.lastStateChangeTime = new Timestamp();
            this.executing = state.equals(TxState.EXECUTING);
        }
    }

    /**
     * Get the {@link Timestamp} of the most recent state change.
     *
     * @return timestamp of most recent state change
     */
    public Timestamp getLastStateChangeTime() {
        synchronized (this.raft) {
            return this.lastStateChangeTime;
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
     * <p>
     * For {@link Consistency#UNCOMMITTED} transactions, this will always return zero.
     *
     * @return associated commit log entry index, or zero if not yet determined
     */
    public long getCommitTerm() {
        synchronized (this.raft) {
            return this.commitTerm;
        }
    }

    /**
     * Get the index of the Raft log entry on which this transaction is waiting to be committed (in the Raft sense)
     * before it can complete.
     *
     * <p>
     * For {@link Consistency#UNCOMMITTED} transactions, this will always return zero.
     *
     * @return associated commit log entry term, or zero if not yet determined
     */
    public long getCommitIndex() {
        synchronized (this.raft) {
            return this.commitIndex;
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
        return this.consistency;
    }

    /**
     * Determine whether this transaction is configured as read-only.
     *
     * <p>
     * Default is false.
     *
     * @return true if this transaction is configured read-only
     */
    @Override
    public boolean isReadOnly() {
        synchronized (this.raft) {
            return this.readOnly;
        }
    }

    /**
     * Configure whether this transaction is the <i>high priority</i> transaction for this node.
     *
     * <p>
     * At most one open transaction on a Raft node can be high priority at a time; if two concurrent transactions are
     * configured as high priority on the same node, the most recent invocation of this method wins.
     *
     * <p>
     * High priority transactions are handled specially on leaders: the high priority transaction always wins when
     * there is a conflict with another transaction, whether the other transaction is local or remote (i.e., sent
     * from a follower), so a high priority transaction can never fail due to a conflict (of course there are other
     * reasons a high priority transaction could still fail, e.g., leadership change or inability to communicate
     * with a majority). So this provides a simple "all or nothing" prioritization scheme.
     *
     * <p>
     * On followers, configuring a transaction as high priority simply forces an immediate leader election, as if by
     * {@link FollowerRole#startElection FollowerRole.startElection()}. This causes the node to become the leader with
     * high probability, where it can then prioritize this transaction as described above (if the transaction detects
     * a conflict before the election completes, a subsequent retry after the completed election should succeed).
     *
     * <p>
     * Warning: overly-agressive use of this method can cause a flurry of elections and poor performance, therefore,
     * this method should be used sparingly, otherwise an election storm could result. In any case, followers will not
     * force a new election for this purpose unless there is an {@linkplain FollowerRole#getLeaderIdentity established leader}.
     *
     * <p>
     * This method is only valid for {@link Consistency#LINEARIZABLE} transactions.
     *
     * <p>
     * Default is false.
     *
     * @param highPriority true for high priority, otherwise false
     * @return true if successful, false if this transaction is no longer alive or is not {@link Consistency#LINEARIZABLE}
     */
    public boolean setHighPriority(boolean highPriority) {
        return this.raft.setHighPriority(this, highPriority);
    }

    /**
     * Determine whether this transaction is the high priority transaction for this node.
     *
     * <p>
     * A high priority transaction is automatically reverted sometime after {@link #commit} is invoked, so this method
     * is only reliable while this transaction in state {@link TxState#EXECUTING}.
     *
     * @return true if high priority, otherwise false
     * @see #setHighPriority
     */
    public boolean isHighPriority() {
        synchronized (this.raft) {
            return this == this.raft.highPrioTx;
        }
    }

    /**
     * Set whether this transaction should be read-only.
     *
     * <p>
     * Read-only transactions support modifications during the transaction, and these modifications will be visible
     * when read back, but they are discarded on {@link #commit commit()}.
     *
     * <p>
     * By default, {@link Consistency#LINEARIZABLE} transactions are read-write. They may be changed to read-only via
     * this method, and once changed, may not be changed back. Non-{@link Consistency#LINEARIZABLE} are always read-only.
     *
     * @param readOnly true to discard mutations on commit, false to apply mutations on commit
     * @throws IllegalArgumentException if {@code readOnly} is false and
     *  {@linkplain #getConsistency this transaction's consistency} is not {@link Consistency#LINEARIZABLE}
     * @throws IllegalArgumentException if {@code readOnly} is false and this transaction is currently read-only
     * @throws StaleKVTransactionException if this transaction is no longer open
     */
    @Override
    public void setReadOnly(final boolean readOnly) {
        synchronized (this.raft) {
            if (readOnly == this.readOnly)
                return;
            Preconditions.checkArgument(readOnly || this.consistency.equals(Consistency.LINEARIZABLE),
              this.consistency + " transactions must be read-only");
            Preconditions.checkArgument(readOnly || !this.consistency.equals(Consistency.LINEARIZABLE),
              this.consistency + " transactions cannot be changed back to read-write after being set read-only");
            assert this.consistency.equals(Consistency.LINEARIZABLE);
            assert !this.readOnly;
            this.verifyExecuting();
            this.readOnly = readOnly;
            this.raft.role.handleLinearizableReadOnlyChange(this);
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
     * @throws IllegalStateException if this transaction is read-only
     * @throws IllegalArgumentException if {@code identity} is null
     */
    public void configChange(String identity, String address) {
        Preconditions.checkArgument(identity != null, "null identity");
        synchronized (this.raft) {
            Preconditions.checkState(this.configChange == null, "duplicate config change; only one is supported per transaction");
            Preconditions.checkState(!this.readOnly, "transaction is read-only");
            this.verifyExecuting();
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
    public ByteData get(ByteData key) {
        this.fastVerifyExecuting();
        return this.view.get(key);
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        this.fastVerifyExecuting();
        return this.view.getAtLeast(minKey, maxKey);
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        this.fastVerifyExecuting();
        return this.view.getAtMost(maxKey, minKey);
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        this.fastVerifyExecuting();
        return this.view.getRange(minKey, maxKey, reverse);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        this.fastVerifyExecuting();
        try {
            this.view.put(key, value);
        } catch (IllegalStateException e) {     // most likely reason: tx is no longer EXECUTING but we just missed the transition
            synchronized (this.raft) {
                this.verifyExecuting();
            }
            throw e;
        }
    }

    @Override
    public void remove(ByteData key) {
        this.fastVerifyExecuting();
        try {
            this.view.remove(key);
        } catch (IllegalStateException e) {     // most likely reason: tx is no longer EXECUTING but we just missed the transition
            synchronized (this.raft) {
                this.verifyExecuting();
            }
            throw e;
        }
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        this.fastVerifyExecuting();
        try {
            this.view.removeRange(minKey, maxKey);
        } catch (IllegalStateException e) {     // most likely reason: tx is no longer EXECUTING but we just missed the transition
            synchronized (this.raft) {
                this.verifyExecuting();
            }
            throw e;
        }
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        this.fastVerifyExecuting();
        try {
            this.view.adjustCounter(key, amount);
        } catch (IllegalStateException e) {     // most likely reason: tx is no longer EXECUTING but we just missed the transition
            synchronized (this.raft) {
                this.verifyExecuting();
            }
            throw e;
        }
    }

    @Override
    public ByteData encodeCounter(long value) {
        return this.view.encodeCounter(value);
    }

    @Override
    public long decodeCounter(ByteData bytes) {
        return this.view.decodeCounter(bytes);
    }

    @Override
    public void apply(Mutations mutations) {
        this.fastVerifyExecuting();
        try {
            this.view.apply(mutations);
        } catch (IllegalStateException e) {     // most likely reason: tx is no longer EXECUTING but we just missed the transition
            synchronized (this.raft) {
                this.verifyExecuting();
            }
            throw e;
        }
    }

    private void fastVerifyExecuting() {
        if (this.executing)
            return;
        synchronized (this.raft) {
            this.verifyExecuting();
        }
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
     * milliseconds, a {@link RetryKVTransactionException} is thrown.
     *
     * <p>
     * The default value for all transactions is configured by {@link RaftKVDatabase#setCommitTimeout}.
     *
     * @param timeout transaction commit timeout in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code timeout} is negative
     * @throws StaleKVTransactionException if this transaction is no longer open
     */
    @Override
    public void setTimeout(final long timeout) {
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        synchronized (this.raft) {
            if (timeout == this.timeout)
                return;
            this.verifyExecuting();
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
     * @throws StaleKVTransactionException {@inheritDoc}
     * @throws io.permazen.kv.RetryKVTransactionException {@inheritDoc}
     * @throws io.permazen.kv.KVDatabaseException {@inheritDoc}
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public ListenableFuture<Void> watchKey(ByteData key) {
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

    @Override
    public void withWeakConsistency(Runnable action) {
        this.view.withoutReadTracking(false, action);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Read-only snapshots are supported by {@link RaftKVTransaction}.
     *
     * @return {@inheritDoc}
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws StaleKVTransactionException {@inheritDoc}
     * @throws io.permazen.kv.RetryKVTransactionException {@inheritDoc}
     */
    @Override
    public CloseableKVStore readOnlySnapshot() {
        final Writes writes;
        synchronized (this.view) {
            writes = this.view.getWrites().clone();
        }
        synchronized (this.raft) {
            this.verifyExecuting();
            assert this.snapshotRefs != null;
            this.snapshotRefs.ref();
            final MutableView snapshotView = new MutableView(this.snapshotRefs.getTarget(), writes);
            snapshotView.setReadOnly();
            return new CloseableForwardingKVStore(snapshotView, this.snapshotRefs::unref);
        }
    }

// Package-access methods

    SettableFuture<Void> getCommitFuture() {
        return this.commitFuture;
    }

    int getTimeout() {
        return this.timeout;
    }

    Timer getCommitTimer() {
        return this.commitTimer;
    }
    void setCommitTimer(final Timer commitTimer) {
        this.commitTimer = commitTimer;
    }

    KVTransactionException getFailure() {
        assert Thread.holdsLock(this.raft);
        return this.failure;
    }

    Timestamp getCommitLeaderLeaseTimeout() {
        return this.commitLeaderLeaseTimeout;
    }

    void setFailure(final KVTransactionException failure) {
        assert Thread.holdsLock(this.raft);
        this.failure = failure;
    }

    boolean hasCommitInfo() {
        assert (this.commitTerm == 0) == (this.commitIndex == 0);
        return this.commitTerm != 0;
    }

    void setCommitInfo(final long commitTerm, final long commitIndex, final Timestamp commitLeaderLeaseTimeout) {
        assert Thread.holdsLock(this.raft);
        assert !this.hasCommitInfo();
        if (this.raft.logger.isTraceEnabled()) {
            this.raft.trace("setting commit to " + commitIndex + "t" + commitTerm
              + (commitLeaderLeaseTimeout != null ? "@" + commitLeaderLeaseTimeout : "") + " for " + this);
        }
        this.commitTerm = commitTerm;
        this.commitIndex = commitIndex;
        this.commitLeaderLeaseTimeout = commitLeaderLeaseTimeout;
        if (this.rebasable && this.baseIndex >= this.commitIndex)
            this.setNoLongerRebasable();
    }

    void rebase(long baseTerm, long baseIndex, KVStore kvstore, CloseableKVStore snapshot) {
        assert Thread.holdsLock(this.view);
        assert Thread.holdsLock(this.raft);
        assert this.state.equals(TxState.EXECUTING);
        assert this.snapshotRefs != null;
        this.rebase(baseTerm, baseIndex);
        this.view.setKVStore(kvstore);
        this.snapshotRefs.unref();
        this.snapshotRefs = new CloseableRefs<>(snapshot);
    }

    void rebase(long baseTerm, long baseIndex) {
        assert baseIndex > this.baseIndex;
        this.setBase(baseTerm, baseIndex);
    }

    void setBase(final long baseTerm, final long baseIndex) {
        assert Thread.holdsLock(this.raft);
        assert this.failure == null;
        this.baseTerm = baseTerm;
        this.baseIndex = baseIndex;
    }

    void verifyExecuting() {
        assert Thread.holdsLock(this.raft);
        if (!this.state.equals(TxState.EXECUTING))
            throw this.failure != null ? this.failure.duplicate() : new StaleKVTransactionException(this);
    }

    boolean isCommittable() {
        assert Thread.holdsLock(this.raft);
        return this.committable;
    }
    void setCommittable() {
        assert Thread.holdsLock(this.raft);
        assert !this.committable;
        this.committable = true;
    }

    boolean isRebasable() {
        assert Thread.holdsLock(this.raft);
        return this.rebasable;
    }
    void setNoLongerRebasable() {
        assert Thread.holdsLock(this.raft);
        assert this.rebasable == (this.view.getReads() != null);
        if (this.rebasable) {
            if (this.raft.logger.isTraceEnabled())
                this.raft.trace("stopping rebasing for " + this);
            this.raft.setHighPriority(this, false);     // if it's not longer rebasable, it can't be a victim of any conflicts
            this.view.disableReadTracking();
            this.rebasable = false;
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
              + "," + this.state
              + ",base=" + this.baseIndex + "t" + this.baseTerm
              + (this.rebasable ? ",rebasable" : "")
              + "," + this.consistency
              + (this.addsLogEntry() ? ",mutating" : !this.readOnly ? ",non-mutating" : "")
              + (this.readOnly ? ",readOnly" : "")
              + (this.configChange != null ? ",config=" + (this.configChange[1] != null ?
               "+" + this.configChange[0] + "@" + this.configChange[1] : "-" + this.configChange[0]) : "")
              + ((this.commitIndex | this.commitTerm) != 0 ? ",commit=" + this.commitIndex + "t" + this.commitTerm : "")
              + (this.committable ? ",committable" : "")
              + (this.timeout != 0 ? ",timeout=" + this.timeout : "")
              + (this.commitLeaderLeaseTimeout != null ? ",leaseTimeout=" + this.commitLeaderLeaseTimeout : "")
              + "]";
        }
    }

    @Override
    @SuppressWarnings("deprecation")
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
        if (this.state.compareTo(TxState.COMPLETED) < 0) {
            assert this.baseTerm <= currentTerm;
            assert this.commitIndex == 0 || this.commitIndex >= this.baseIndex;
        }
        assert this.commitTerm <= currentTerm;
        assert !this.committable || this.commitTerm > 0 || this.commitIndex > 0 || this.consistency == Consistency.UNCOMMITTED;
        assert this.commitLeaderLeaseTimeout == null || this.commitTerm > 0 || this.commitIndex > 0;
        assert this.raft.role.checkRebasableAndCommittableUpToDate(this);
        assert !this.rebasable || !this.committable;
        assert !this.rebasable || this.baseIndex >= lastIndex;
        assert !this.rebasable || this.consistency == Consistency.LINEARIZABLE;
        assert !this.rebasable || this.commitIndex == 0 || this.commitIndex > raftCommitIndex;
        assert !this.rebasable || this.commitIndex == 0 || !this.addsLogEntry();
        assert this.rebasable == (this.view.getReads() != null);
        assert this.rebasable || this != this.raft.highPrioTx;
        assert this.consistency == Consistency.LINEARIZABLE || this != this.raft.highPrioTx;
        switch (this.consistency) {
        case LINEARIZABLE:
            assert !this.committable || this.addsLogEntry() || this.baseIndex == this.commitIndex
              || this.state.compareTo(TxState.COMMIT_WAITING) >= 0;
            assert !this.committable || !this.addsLogEntry() || this.baseIndex < this.commitIndex;
            assert this.commitIndex == 0 || !this.addsLogEntry() || this.state.compareTo(TxState.COMMIT_WAITING) >= 0;
            assert this.commitTerm == 0 || this.commitTerm >= this.baseTerm;
            assert this.commitIndex == 0 || this.commitIndex >= this.baseIndex;
            break;
        case EVENTUAL:
            assert this.readOnly;
            assert !this.rebasable;
            assert this.configChange == null;
            assert this.commitTerm == this.baseTerm;
            assert this.commitIndex == this.baseIndex;
            break;
        case EVENTUAL_COMMITTED:
            assert this.readOnly;
            assert !this.rebasable;
            assert this.configChange == null;
            assert this.commitTerm == this.baseTerm;
            assert this.commitIndex == this.baseIndex;
            assert this.commitIndex <= raftCommitIndex;
            assert this.committable;
            break;
        case UNCOMMITTED:
            assert this.readOnly;
            assert !this.rebasable;
            assert this.configChange == null;
            assert this.commitTerm == 0;
            assert this.commitIndex == 0;
            assert this.committable;
            assert raftCommitIndex >= this.commitIndex;
            break;
        default:
            assert false;
            break;
        }
        assert this.readOnly || this.consistency.equals(Consistency.LINEARIZABLE);
        assert !this.readOnly || this.configChange == null;
        switch (this.state) {
        case EXECUTING:
            assert this.snapshotRefs != null;
            assert this.failure == null;
            break;
        case COMMIT_READY:
            assert this.snapshotRefs == null;
            assert this.failure == null;
            break;
        case COMMIT_WAITING:
            assert this.commitTerm <= currentTerm;
            if (this.consistency != Consistency.UNCOMMITTED) {
                assert this.commitTerm >= this.baseTerm;
                assert this.commitIndex >= this.baseIndex;
            }
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
