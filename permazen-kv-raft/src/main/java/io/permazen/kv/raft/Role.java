
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import io.permazen.kv.KVTransactionException;
import io.permazen.kv.KeyRange;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.mvcc.Conflict;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.mvcc.Reads;
import io.permazen.kv.mvcc.TransactionConflictException;
import io.permazen.kv.mvcc.Writes;
import io.permazen.kv.raft.msg.AppendRequest;
import io.permazen.kv.raft.msg.AppendResponse;
import io.permazen.kv.raft.msg.CommitRequest;
import io.permazen.kv.raft.msg.CommitResponse;
import io.permazen.kv.raft.msg.GrantVote;
import io.permazen.kv.raft.msg.InstallSnapshot;
import io.permazen.kv.raft.msg.Message;
import io.permazen.kv.raft.msg.PingRequest;
import io.permazen.kv.raft.msg.PingResponse;
import io.permazen.kv.raft.msg.RequestVote;
import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;

/**
 * Common superclass for the three roles played by a Raft node:
 * {@linkplain LeaderRole leader}, {@linkplain FollowerRole follower}, and {@linkplain CandidateRole candidate}.
 */
public abstract class Role {

    final Logger log;
    final RaftKVDatabase raft;
    final Service checkReadyTransactionsService = new Service(this, "check ready transactions", this::checkReadyTransactions);
    final Service checkWaitingTransactionsService = new Service(this, "check waiting transactions", this::checkWaitingTransactions);

    // NOTE: use of this service requires that 'checkWaitingTransactionsService' be scheduled first!
    final Service applyCommittedLogEntriesService = new Service(this, "apply committed logs", this::applyCommittedLogEntries);
    final Service triggerKeyWatchesService = new Service(this, "trigger key watches", this::triggerKeyWatches);

// Constructors

    Role(RaftKVDatabase raft) {
        this.raft = raft;
        this.log = this.raft.logger;
        assert Thread.holdsLock(this.raft);
    }

// Status

    /**
     * Get the {@link RaftKVDatabase} with which this instance is associated.
     *
     * @return associated database
     */
    public RaftKVDatabase getKVDatabase() {
        return this.raft;
    }

// Lifecycle

    void setup() {
        assert Thread.holdsLock(this.raft);
        this.raft.requestService(this.checkReadyTransactionsService);
        this.raft.requestService(this.checkWaitingTransactionsService);
        this.raft.requestService(this.applyCommittedLogEntriesService);
    }

    void shutdown() {

        // Sanity check
        assert Thread.holdsLock(this.raft);

        // Fail any (read-only) transactions with a minimum lease timeout, because they won't be valid for a new leader
        for (RaftKVTransaction tx : new ArrayList<>(this.raft.openTransactions.values())) {
            if (!tx.getState().equals(TxState.COMPLETED) && tx.getCommitLeaderLeaseTimeout() != null) {
                assert tx.hasCommitInfo();
                this.raft.fail(tx, new RetryKVTransactionException(tx, "leader was deposed during leader lease timeout wait"));
            }
        }

        // Cleanup role-specific state
        for (RaftKVTransaction tx : this.raft.openTransactions.values())
            this.cleanupForTransaction(tx);
    }

// Service

    abstract void outputQueueEmpty(String address);

    /**
     * Check transactions in the {@link TxState#COMMIT_READY} state to see if we can advance them.
     */
    void checkReadyTransactions() {
        assert Thread.holdsLock(this.raft);
        for (RaftKVTransaction tx : new ArrayList<>(this.raft.openTransactions.values())) {
            if (TxState.COMMIT_READY.equals(tx.getState()))
                new CheckReadyTransactionService(this, tx).run();
        }
    }

    /**
     * Check transactions in the {@link TxState#COMMIT_WAITING} state to see if they are committed yet.
     * We invoke this service method whenever our {@code commitIndex} advances.
     */
    void checkWaitingTransactions() {
        assert Thread.holdsLock(this.raft);
        for (RaftKVTransaction tx : new ArrayList<>(this.raft.openTransactions.values())) {
            if (TxState.COMMIT_WAITING.equals(tx.getState()))
                new CheckWaitingTransactionService(this, tx).run();
        }
    }

    /**
     * Apply committed but unapplied log entries to the state machine.
     *
     * <p>
     * We invoke this service method whenever our {@code commitIndex} advances.
     *
     * <p>
     * Note: checkWaitingTransactions() must have been invoked already when this method is invoked.
     *
     * <p>
     * In addition, we discard applied log entries that are no longer needed (based on {@link #calculateMaxAppliedDiscardIndex}).
     */
    void applyCommittedLogEntries() {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert this.checkRebasableAndCommittableUpToDate();
        assert this.raft.commitIndex >= this.raft.log.getLastAppliedIndex();

        // Apply all committed log entries to the state machine
        boolean anyApplied = false;
        while (this.raft.log.getLastAppliedIndex() < this.raft.commitIndex) {

            // Grab the first unwritten log entry
            final LogEntry logEntry = this.raft.log.getUnapplied().get(0);
            assert logEntry.getIndex() == this.raft.log.getLastAppliedIndex() + 1;

            // Get the current config as of the log entry we're about to apply
            final HashMap<String, String> logEntryConfig = new HashMap<>(this.raft.log.getLastAppliedConfig());
            logEntry.applyConfigChange(logEntryConfig);

            // Prepare combined Mutations containing prefixed log entry changes plus my own
            final Mutations logWrites = logEntry.getWrites();
            final Writes myWrites = new Writes();
            myWrites.getPuts().put(RaftKVDatabase.LAST_APPLIED_TERM_KEY, LongEncoder.encode(logEntry.getTerm()));
            myWrites.getPuts().put(RaftKVDatabase.LAST_APPLIED_INDEX_KEY, LongEncoder.encode(logEntry.getIndex()));
            myWrites.getPuts().put(RaftKVDatabase.LAST_APPLIED_CONFIG_KEY, this.raft.encodeConfig(logEntryConfig));
            final ByteData stateMachinePrefix = this.raft.getStateMachinePrefix();
            final Mutations mutations = new Mutations() {

                @Override
                public Stream<KeyRange> getRemoveRanges() {
                    return logWrites.getRemoveRanges().map(range -> range.prefixedBy(stateMachinePrefix));
                }

                @Override
                public Stream<Map.Entry<ByteData, ByteData>> getPutPairs() {
                    return Stream.concat(
                      logWrites.getPutPairs().map(entry -> new AbstractMap.SimpleEntry<>(
                        stateMachinePrefix.concat(entry.getKey()), entry.getValue())),
                      myWrites.getPutPairs());
                }

                @Override
                public Stream<Map.Entry<ByteData, Long>> getAdjustPairs() {
                    return logWrites.getAdjustPairs()
                      .map(entry -> new AbstractMap.SimpleEntry<>(
                        stateMachinePrefix.concat(entry.getKey()), entry.getValue()));
                }
            };

            // Apply updates to the key/value store; when applying the last one, durably persist
            if (this.log.isDebugEnabled())
                this.debug("applying committed log entry {} to key/value store", logEntry);
            try {
                this.raft.kv.apply(mutations,
                  !this.raft.disableSync && this.raft.log.getLastAppliedIndex() == this.raft.commitIndex);
            } catch (Exception e) {
                final Throwable cause = e.getCause() instanceof IOException ? (IOException)e.getCause() : e;
                this.error("error applying log entry {} to key/value store", logEntry, cause);
                break;
            }

            // Update log
            this.raft.log.applyNextLogEntry();
            anyApplied = true;
            assert this.raft.currentConfig.equals(this.raft.log.buildCurrentConfig());
        }

        // Discard already-applied log entries
        if (anyApplied)
            this.raft.log.discardAppliedLogEntries(this.calculateMaxAppliedDiscardIndex());
    }

    /**
     * Calculate the maximum index (inclusive) of applied log entries we may safely discard.
     */
    long calculateMaxAppliedDiscardIndex() {
        return this.raft.log.getLastAppliedIndex();
    }

    // Assertion check
    boolean checkRebasableAndCommittableUpToDate() {
        for (RaftKVTransaction tx : this.raft.openTransactions.values())
            this.checkRebasableAndCommittableUpToDate(tx);
        return true;
    }

    // Assertion check
    boolean checkRebasableAndCommittableUpToDate(RaftKVTransaction tx) {

        // A rebasable transactions should be fully rebased
        assert !tx.isRebasable() || tx.getBaseIndex() == this.raft.log.getLastIndex() : "rebasable check failed for " + tx;

        // A committable transaction should be marked as such
        if (!tx.isCommittable()) {
            try {
                assert !this.checkCommittable(tx);
            } catch (KVTransactionException e) {
                // ok - it's not committable because it's broken
            }
        }
        return true;
    }

    /**
     * Trigger any key watches for changes in log entries committed since the last time we checked.
     *
     * <p>
     * This should be invoked:
     * <ul>
     *  <li>After advancing the commitIndex</li>
     *  <li>After resetting the state machine</li>
     *  <li>After installing a snapshot</li>
     * </ul>
     */
    void triggerKeyWatches() {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert this.raft.commitIndex >= this.raft.log.getLastAppliedIndex();
        assert this.raft.commitIndex <= this.raft.log.getLastIndex();
        assert this.raft.keyWatchIndex <= this.raft.commitIndex;

        // If nobody is watching, don't bother
        if (this.raft.keyWatchTracker == null)
            return;

        // If we have received a snapshot install, we may not be able to tell which keys have changed since last notification;
        // in that case, trigger all key watches; otherwise, trigger the keys affected by newly committed log entries
        if (this.raft.keyWatchIndex < this.raft.log.getLastAppliedIndex()) {
            this.raft.keyWatchTracker.triggerAll();
            this.raft.keyWatchIndex = this.raft.commitIndex;
        } else {
            while (this.raft.keyWatchIndex < this.raft.commitIndex)
                this.raft.keyWatchTracker.trigger(this.raft.log.getEntryAtIndex(++this.raft.keyWatchIndex).getWrites());
        }
    }

// Transactions

    /**
     * Handle the situation where a {@link Consistency#LINEARIZABLE} transaction in state {@link TxState#EXECUTING}
     * transitions from read-write to read-only.
     */
    void handleLinearizableReadOnlyChange(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getState().equals(TxState.EXECUTING);
        assert tx.getConsistency().equals(Consistency.LINEARIZABLE);
        assert tx.isReadOnly();
        assert !tx.hasCommitInfo();
        assert tx.isRebasable();
        assert !tx.isCommittable();
        assert this.checkRebasableAndCommittableUpToDate(tx);
    }

    /**
     * Check a transaction that is ready to be committed (in the {@link TxState#COMMIT_READY} state).
     *
     * <p>
     * This should be invoked:
     * <ul>
     *  <li>After changing roles</li>
     *  <li>After a transaction has entered the {@link TxState#COMMIT_READY} state</li>
     *  <li>After the leader is newly known (in {@link FollowerRole})</li>
     *  <li>After the leader's output queue goes from non-empty to empty (in {@link FollowerRole})</li>
     *  <li>After the leader's {@code commitIndex} has advanced, in case a config change transaction
     *      is waiting on a previous config change transaction (in {@link LeaderRole})</li>
     * </ul>
     *
     * @param tx the transaction
     * @throws KVTransactionException if an error occurs
     */
    final void checkReadyTransaction(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getState().equals(TxState.COMMIT_READY);

        // If transaction already has a commit term & index, proceed to COMMIT_WAITING
        if (tx.hasCommitInfo()) {
            this.advanceReadyTransaction(tx);
            return;
        }

        // Requires leader communication to acquire commit term+index - let subclass handle it
        assert !tx.isCommittable();
        assert tx.getConsistency().equals(Consistency.LINEARIZABLE);
        this.checkReadyTransactionNeedingCommitInfo(tx);
    }

    /**
     * Handle a linearizable transaction that is ready to be committed (in the {@link TxState#COMMIT_READY} state) but
     * does not yet have a commit term &amp; index and therefore requires communication with the leader.
     *
     * @param tx the transaction
     * @throws KVTransactionException if an error occurs
     */
    void checkReadyTransactionNeedingCommitInfo(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getState().equals(TxState.COMMIT_READY);
        assert tx.getConsistency().equals(Consistency.LINEARIZABLE);
        assert !tx.hasCommitInfo();
        assert !tx.isCommittable();
        assert this.checkRebasableAndCommittableUpToDate(tx);
    }

    /**
     * Advance a transaction from the {@link TxState#COMMIT_READY} state to the {@link TxState#COMMIT_WAITING} state.
     *
     * @param tx the transaction
     * @param commitTerm term of log entry that must be committed before the transaction may succeed
     * @param commitIndex index of log entry that must be committed before the transaction may succeed
     * @param commitLeaderLeaseTimeout if not null, minimum leader lease timeout we must see before commit may succeed
     */
    final void advanceReadyTransactionWithCommitInfo(RaftKVTransaction tx,
      long commitTerm, long commitIndex, Timestamp commitLeaderLeaseTimeout) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getState().equals(TxState.COMMIT_READY);
        assert !tx.hasCommitInfo();

        // Set commit term & index
        tx.setCommitInfo(commitTerm, commitIndex, commitLeaderLeaseTimeout);

        // Advance to COMMIT_WAITING
        this.advanceReadyTransaction(tx);
    }

    /**
     * Advance a transaction from the {@link TxState#COMMIT_READY} state to the {@link TxState#COMMIT_WAITING} state.
     *
     * <p>
     * This assumes the commit info is already set.
     *
     * @param tx the transaction
     */
    final void advanceReadyTransaction(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getState().equals(TxState.COMMIT_READY);
        assert tx.hasCommitInfo();

        // Update state
        if (this.log.isTraceEnabled())
            this.trace("advancing {} to {}", tx, TxState.COMMIT_WAITING);
        tx.setState(TxState.COMMIT_WAITING);
        tx.setNoLongerRebasable();
        this.checkCommittable(tx);

        // Check this transaction to see if it can be committed
        new CheckWaitingTransactionService(this, tx).run();
    }

    /**
     * Check a transaction waiting for its log entry to be committed (in the {@link TxState#COMMIT_WAITING} state).
     *
     * <p>
     * This should be invoked:
     * <ul>
     *  <li>After changing roles</li>
     *  <li>After a transaction has entered the {@link TxState#COMMIT_WAITING} state</li>
     *  <li>After a transaction has been rebased</li>
     *  <li>After advancing my {@code commitIndex} (as leader or follower)</li>
     *  <li>After receiving an updated {@linkplain AppendRequest#getLeaderLeaseTimeout leader lease timeout}
     *      (in {@link FollowerRole})</li>
     *  <li>After updating my {@linkplain AppendRequest#getLeaderLeaseTimeout leader lease timeout} (in {@link LeaderRole})</li>
     * </ul>
     *
     * @param tx the transaction
     * @throws KVTransactionException if an error occurs
     */
    final void checkWaitingTransaction(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getConsistency().isGuaranteesUpToDateReads();

        // Is transaction committable?
        if (!this.checkCommittable(tx))
            return;

        // Is there a required minimum leader lease timeout associated with the transaction? If so, we must wait for it.
        final Timestamp commitLeaderLeaseTimeout = tx.getCommitLeaderLeaseTimeout();
        if (commitLeaderLeaseTimeout != null && !this.isLeaderLeaseActiveAt(commitLeaderLeaseTimeout)) {
            if (this.log.isTraceEnabled())
                this.trace("committable {} must wait for leader lease timeout {}", tx, commitLeaderLeaseTimeout);
            return;
        }

        // Allow transaction commit to complete
        if (this.log.isTraceEnabled())
            this.trace("commit successful for {}", tx);
        this.raft.succeed(tx);
    }

    /**
     * Detect newly-committable transactions.
     *
     * <p>
     * This should be invoked after advancing my {@code commitIndex} (as leader or follower).
     */
    void checkCommittables() {

        // Sanity check
        assert Thread.holdsLock(this.raft);

        // Check which transactions are now committable
        for (RaftKVTransaction tx : new ArrayList<>(this.raft.openTransactions.values())) {
            try {
                this.checkCommittable(tx);
            } catch (KVTransactionException e) {
                this.raft.fail(tx, e);
            } catch (Exception | Error e) {
                this.raft.error("error checking committable for transaction " + tx, e);
                this.raft.fail(tx, new KVTransactionException(tx, e));
            }
        }
    }

    /**
     * Determine if a transaction has become committable, and mark it so if so.
     *
     * <p>
     * This should be invoked after advancing my {@code commitIndex} (as leader or follower), after setting
     * the commit info for a transaction, or after rebasing a transaction that has commit info already.
     *
     * <p>
     * Note: "committable" means ready to commit except any required wait for {@code tx.commitLeaderLeaseTimeout}.
     * In particular, the commit term+index is known, the corresponding log entry has been committed, and if rebasable
     * the transaction is rebased up through the commit term+index.
     *
     * @param tx the transaction
     * @throws KVTransactionException if an error occurs
     */
    boolean checkCommittable(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);

        // Already checked?
        if (tx.isCommittable())
            return true;

        // Has the transaction's commit info been determined yet?
        final long commitIndex = tx.getCommitIndex();
        final long commitTerm = tx.getCommitTerm();
        if (commitIndex == 0)
            return false;

        // Has the transaction's commit log entry been added yet?
        final long lastIndex = this.raft.log.getLastIndex();
        if (commitIndex > lastIndex)
            return false;

        // Compare commit term to the actual term of the commit log entry
        final long commitIndexActualTerm = this.raft.log.getTermAtIndexIfKnown(commitIndex);
        if (commitIndexActualTerm == 0) {

            // The commit log entry has already been forgotten. We don't know whether it actually got committed
            // or not, so the transaction must be retried.
            throw new RetryKVTransactionException(tx, String.format(
              "commit index %d < first index %d for which the term is known",
              commitIndex, this.raft.log.getFirstIndex()));
        }

        // Verify the term of the committed log entry; if not what we expect, the log entry was overwritten by a new leader
        if (commitTerm != commitIndexActualTerm) {
            throw new RetryKVTransactionException(tx, String.format(
              "leader was deposed during commit and transaction's commit log entry %dt%d overwritten by %dt%d",
              commitIndex, commitTerm, commitIndex, commitIndexActualTerm));
        }

        // Has the transaction's commit log entry been committed yet?
        if (commitIndex > this.raft.commitIndex)
            return false;

        // If transaction is rebasable, it must be rebased at least up through its commit index
        if (tx.isRebasable() && tx.getBaseIndex() < commitIndex)
            return false;

        // The transaction's commit log entry is committed, so mark the transaction as committable
        if (this.log.isTraceEnabled())
            this.trace("{} is now committable: {} >= {}t{}", tx, this.raft.commitIndex, commitIndex, commitTerm);
        tx.setCommittable();
        if (tx.isRebasable())
            tx.setNoLongerRebasable();
        return true;
    }

    /**
     * Rebase all rebasable transactions up to through the last log entry.
     *
     * <p>
     * We only rebase {@link Consistency#LINEARIZABLE} transactions that are either non-mutating or have not
     * yet had a {@link CommitRequest} sent to the leader.
     *
     * <p>
     * This should be invoked after appending a new Raft log entry.
     *
     * @param highPrioAlreadyChecked if the high priority transaction is already checked for conflicts
     * @throws KVTransactionException if an error occurs
     */
    void rebaseTransactions(boolean highPrioAlreadyChecked) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert !highPrioAlreadyChecked || this.raft.highPrioTx != null;
        assert !highPrioAlreadyChecked || Thread.holdsLock(this.raft.highPrioTx.view);

        // Rebase all rebasable transactions
        for (RaftKVTransaction tx : new ArrayList<>(this.raft.openTransactions.values())) {
            if (!tx.isRebasable())
                continue;
            try {
                this.rebaseTransaction(tx, highPrioAlreadyChecked && tx == this.raft.highPrioTx);
            } catch (KVTransactionException e) {
                this.raft.fail(tx, e);
            } catch (Exception | Error e) {
                this.raft.error("error rebasing transaction " + tx, e);
                this.raft.fail(tx, new KVTransactionException(tx, e));
            }
        }
    }

    /**
     * Rebase the given transaction so that its base log entry is the last log entry or its commit log entry,
     * whichever is lower.
     *
     * <p>
     * This should be invoked for each {@linkplain RaftKVTransaction#isRebasable rebasable} transaction
     * after appending a new log entry.
     *
     * <p>
     * This method assumes that the given transaction is {@linkplain RaftKVTransaction#isRebasable rebasable}.
     *
     * @param tx the transaction
     * @param skipConflictCheck true to skip the conflict check because we've already done it
     * @throws KVTransactionException if an error occurs
     */
    private void rebaseTransaction(RaftKVTransaction tx, boolean skipConflictCheck) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.isRebasable();
        assert tx.getFailure() == null;
        assert tx.getBaseIndex() >= this.raft.log.getLastAppliedIndex();
        assert !tx.hasCommitInfo() || tx.getCommitIndex() > tx.getBaseIndex();
        assert !tx.hasCommitInfo() || !tx.addsLogEntry();

        // Anything to do?
        long baseIndex = tx.getBaseIndex();
        final long lastIndex = this.raft.log.getLastIndex();
        if (baseIndex == lastIndex)
            return;

        // Lock the mutable view so the rebase appears to happen instantaneously to any threads viewing the transaction
        synchronized (tx.view) {

            // Check for conflicts between transaction reads and newly committed log entries
            while (baseIndex < lastIndex) {

                // Check for conflicts
                final LogEntry logEntry = this.raft.log.getEntryAtIndex(++baseIndex);
                assert !skipConflictCheck || !tx.view.getReads().isConflict(logEntry.getWrites());
                final Conflict conflict;
                if (!skipConflictCheck && (conflict = tx.view.getReads().findConflict(logEntry.getWrites())) != null) {
                    if (this.log.isDebugEnabled())
                        this.debug("cannot rebase {} past {}, failing: {}", tx, logEntry, conflict);
                    if (this.raft.dumpConflicts) {
                        this.dumpConflicts(tx.view.getReads(), logEntry.getWrites(),
                          "local txId=" + tx.txId + " fails due to conflicts with " + logEntry);
                    }
                    throw new TransactionConflictException(tx, conflict, String.format(
                      "writes of committed transaction at index %d conflict with reads from transaction base index %d: %s",
                      baseIndex, tx.getBaseIndex(), conflict));
                }

                // If we reach the transaction's commit log entry (if any), we can stop
                if (baseIndex == tx.getCommitIndex()) {
                    tx.setNoLongerRebasable();
                    break;
                }
            }

            // Update transaction
            final long baseTerm = this.raft.log.getTermAtIndex(baseIndex);
            if (this.log.isDebugEnabled())
                this.debug("rebased {} from {}t{} -> {}t{}", tx, tx.getBaseIndex(), tx.getBaseTerm(), baseIndex, baseTerm);
            switch (tx.getState()) {
            case EXECUTING:
                assert !tx.hasCommitInfo() || tx.isReadOnly();
                final MostRecentView view = new MostRecentView(this.raft, baseIndex);
                assert view.getTerm() == baseTerm;
                assert view.getIndex() == baseIndex;
                tx.rebase(baseTerm, baseIndex, view.getView().getBaseKVStore(), view.getSnapshot());
                break;
            case COMMIT_READY:
                tx.rebase(baseTerm, baseIndex);
                break;
            case COMMIT_WAITING:
                tx.rebase(baseTerm, baseIndex);
                this.checkWaitingTransaction(tx);               // transaction might have become committable
                break;
            default:
                throw new RuntimeException("internal error");
            }
        }

        // Check whether transaction has become committable
        if (baseIndex == tx.getCommitIndex())
            this.checkCommittable(tx);
    }

    void dumpConflicts(Reads reads, Mutations writes, String description) {
        final StringBuilder buf = new StringBuilder();
        buf.append(description).append(':');
        for (String conflict : reads.getConflicts(writes))
            buf.append("\n  ").append(conflict);
        this.info(buf.toString());
    }

    /**
     * Get the leader's lease timeout, if known.
     *
     * @return leader lease timeout, or null if unknown
     */
    Timestamp getLeaderLeaseTimeout() {
        return null;
    }

    /**
     * Determine whether the leader's lease timeout extends past the current time, that is, it is known that if
     * the current leader is deposed by a new leader, then that deposition must occur after now.
     *
     * @return true if it is known that no other leader can possibly have been elected at the current time, otherwise false
     */
    protected boolean isLeaderLeaseActiveNow() {
        return this.isLeaderLeaseActiveAt(new Timestamp());
    }

    /**
     * Determine whether the leader's lease timeout extends past the given time, that is, it is known that if
     * the current leader is deposed by a new leader, then that deposition must occur after the given time.
     *
     * @param time leader timestamp
     * @return true if it is known that no other leader can possibly have been elected at the given time, otherwise false
     */
    protected boolean isLeaderLeaseActiveAt(Timestamp time) {
        final Timestamp leaderLeaseTimeout = this.getLeaderLeaseTimeout();
        return leaderLeaseTimeout != null && leaderLeaseTimeout.compareTo(time) > 0;
    }

    /**
     * Perform any role-specific transaction cleanup for the given transaction.
     *
     * <p>
     * Invoked either when transaction is completed OR this role is being shutdown.
     * This method MAY be invoked more than once for the same transaction; it should be idempotent.
     *
     * <p>
     * Subclasses should invoke this method if overriden.
     *
     * @param tx the transaction
     */
    void cleanupForTransaction(RaftKVTransaction tx) {
        assert Thread.holdsLock(this.raft);
    }

// Messages

    // This is a package access version of "implements MessageSwitch"
    abstract void caseAppendRequest(AppendRequest msg, NewLogEntry newLogEntry);
    abstract void caseAppendResponse(AppendResponse msg);
    abstract void caseCommitRequest(CommitRequest msg, NewLogEntry newLogEntry);
    abstract void caseCommitResponse(CommitResponse msg);
    abstract void caseGrantVote(GrantVote msg);
    abstract void caseInstallSnapshot(InstallSnapshot msg);
    abstract void caseRequestVote(RequestVote msg);

    void casePingRequest(PingRequest msg) {
        assert Thread.holdsLock(this.raft);
        final int responseClusterId = this.raft.clusterId != 0 ? this.raft.clusterId : msg.getClusterId();
        this.raft.sendMessage(new PingResponse(responseClusterId,
          this.raft.identity, msg.getSenderId(), this.raft.currentTerm, msg.getTimestamp()));
    }

    void casePingResponse(PingResponse msg) {
        assert Thread.holdsLock(this.raft);
        // ignore by default
    }

    boolean mayAdvanceCurrentTerm(Message msg) {
        return true;
    }

    void failUnexpectedMessage(Message msg) {
        this.warn("rec'd unexpected message {} while in role {}; ignoring", msg, this);
    }

// Debug

    abstract boolean checkState();

    void checkTransaction(RaftKVTransaction tx) {
        this.checkRebasableAndCommittableUpToDate(tx);
    }

// Logging

    void trace(String msg, Object... args) {
        this.raft.trace(msg, args);
    }

    void debug(String msg, Object... args) {
        this.raft.debug(msg, args);
    }

    void info(String msg, Object... args) {
        this.raft.info(msg, args);
    }

    void warn(String msg, Object... args) {
        this.raft.warn(msg, args);
    }

    void error(String msg, Object... args) {
        this.raft.error(msg, args);
    }

    void perfLog(String msg, Object... args) {
        this.raft.perfLog(msg, args);
    }

// Object

    @Override
    public abstract String toString();

    String toStringPrefix() {
        assert Thread.holdsLock(this.raft);
        return this.getClass().getSimpleName()
          + "[term=" + this.raft.currentTerm
          + ",applied=" + this.raft.log.getLastAppliedIndex() + "t" + this.raft.log.getLastAppliedTerm()
          + ",commit=" + this.raft.commitIndex
          + "]";
    }
}
