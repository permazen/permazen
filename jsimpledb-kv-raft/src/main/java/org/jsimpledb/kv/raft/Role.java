
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.kv.mvcc.Reads;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.raft.msg.AppendRequest;
import org.jsimpledb.kv.raft.msg.AppendResponse;
import org.jsimpledb.kv.raft.msg.CommitRequest;
import org.jsimpledb.kv.raft.msg.CommitResponse;
import org.jsimpledb.kv.raft.msg.GrantVote;
import org.jsimpledb.kv.raft.msg.InstallSnapshot;
import org.jsimpledb.kv.raft.msg.Message;
import org.jsimpledb.kv.raft.msg.PingRequest;
import org.jsimpledb.kv.raft.msg.PingResponse;
import org.jsimpledb.kv.raft.msg.RequestVote;
import org.jsimpledb.util.LongEncoder;
import org.slf4j.Logger;

/**
 * Common superclass for the three roles played by a Raft node:
 * {@linkplain LeaderRole leader}, {@linkplain FollowerRole follower}, and {@linkplain CandidateRole candidate}.
 */
public abstract class Role {

    final Logger log;
    final RaftKVDatabase raft;
    final Service checkReadyTransactionsService = new Service(this, "check ready transactions") {
        @Override
        public void run() {
            Role.this.checkReadyTransactions();
        }
    };
    final Service checkWaitingTransactionsService = new Service(this, "check waiting transactions") {
        @Override
        public void run() {
            Role.this.checkWaitingTransactions();
        }
    };

    // NOTE: use of this service requires that 'checkWaitingTransactionsService' be scheduled first!
    final Service applyCommittedLogEntriesService = new Service(this, "apply committed logs") {
        @Override
        public void run() {
            Role.this.applyCommittedLogEntries();
        }
    };
    final Service triggerKeyWatchesService = new Service(this, "trigger key watches") {
        @Override
        public void run() {
            Role.this.triggerKeyWatches();
        }
    };

// Constructors

    Role(RaftKVDatabase raft) {
        this.raft = raft;
        this.log = this.raft.log;
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
        assert Thread.holdsLock(this.raft);
        this.raft.openTransactions.values()
          .forEach(this::cleanupForTransaction);
    }

// Service

    abstract void outputQueueEmpty(String address);

    /**
     * Check transactions in the {@link TxState#COMMIT_READY} state to see if we can advance them.
     */
    void checkReadyTransactions() {
        assert Thread.holdsLock(this.raft);
        for (RaftKVTransaction tx : new ArrayList<>(this.raft.openTransactions.values()))
            new CheckReadyTransactionService(this, tx).run();
    }

    /**
     * Check transactions in the {@link TxState#COMMIT_WAITING} state to see if they are committed yet.
     * We invoke this service method whenever our {@code commitIndex} advances.
     */
    void checkWaitingTransactions() {
        assert Thread.holdsLock(this.raft);
        for (RaftKVTransaction tx : new ArrayList<>(this.raft.openTransactions.values()))
            new CheckWaitingTransactionService(this, tx).run();
    }

    /**
     * Apply committed but unapplied log entries to the state machine.
     * We invoke this service method whenever log entries are added or our {@code commitIndex} advances.
     *
     * <p>
     * Note: checkWaitingTransactions() must have been invoked already when this method is invoked.
     */
    void applyCommittedLogEntries() {
        assert Thread.holdsLock(this.raft);

        // Determine how many committed log entries we can apply to the state machine at this time
        int numEntriesToApply = 0;
        while (this.raft.lastAppliedIndex + numEntriesToApply < this.raft.commitIndex
          && this.mayApplyLogEntry(this.raft.raftLog.get(numEntriesToApply)))
            numEntriesToApply++;
        final long maxAppliedIndex = this.raft.lastAppliedIndex + numEntriesToApply;
        assert maxAppliedIndex <= this.raft.commitIndex;

        // Sanity check that all committable transactions have been committed before we compact their commit log entries
        boolean assertionsEnabled = false;
        assert assertionsEnabled = true;
        if (assertionsEnabled) {
            for (RaftKVTransaction tx : this.raft.openTransactions.values())
                assert !tx.getState().equals(TxState.COMMIT_WAITING) || tx.getCommitIndex() > this.raft.commitIndex;
        }

        // Apply committed log entries to the state machine
        while (this.raft.lastAppliedIndex < maxAppliedIndex) {

            // Grab the first unwritten log entry
            final LogEntry logEntry = this.raft.raftLog.get(0);
            assert logEntry.getIndex() == this.raft.lastAppliedIndex + 1;

            // Get the current config as of the log entry we're about to apply
            final HashMap<String, String> logEntryConfig = new HashMap<>(this.raft.lastAppliedConfig);
            logEntry.applyConfigChange(logEntryConfig);

            // Prepare combined Mutations containing prefixed log entry changes plus my own
            final Writes logWrites = logEntry.getWrites();
            final Writes myWrites = new Writes();
            myWrites.getPuts().put(RaftKVDatabase.LAST_APPLIED_TERM_KEY, LongEncoder.encode(logEntry.getTerm()));
            myWrites.getPuts().put(RaftKVDatabase.LAST_APPLIED_INDEX_KEY, LongEncoder.encode(logEntry.getIndex()));
            myWrites.getPuts().put(RaftKVDatabase.LAST_APPLIED_CONFIG_KEY, this.raft.encodeConfig(logEntryConfig));
            final byte[] stateMachinePrefix = this.raft.getStateMachinePrefix();
            final Mutations mutations = new Mutations() {

                @Override
                public Iterable<KeyRange> getRemoveRanges() {
                    return Iterables.transform(logWrites.getRemoveRanges(), new PrefixKeyRangeFunction(stateMachinePrefix));
                }

                @Override
                public Iterable<Map.Entry<byte[], byte[]>> getPutPairs() {
                    return Iterables.concat(
                      Iterables.transform(logWrites.getPutPairs(), new PrefixPutFunction(stateMachinePrefix)),
                      myWrites.getPutPairs());
                }

                @Override
                public Iterable<Map.Entry<byte[], Long>> getAdjustPairs() {
                    return Iterables.transform(logWrites.getAdjustPairs(), new PrefixAdjustFunction(stateMachinePrefix));
                }
            };

            // Apply updates to the key/value store; when applying the last one, durably persist
            if (this.log.isDebugEnabled())
                this.debug("applying committed log entry " + logEntry + " to key/value store");
            try {
                this.raft.kv.mutate(mutations, !this.raft.disableSync && this.raft.lastAppliedIndex == maxAppliedIndex);
            } catch (Exception e) {
                if (e instanceof RuntimeException && e.getCause() instanceof IOException)
                    e = (IOException)e.getCause();
                this.error("error applying log entry " + logEntry + " to key/value store", e);
                break;
            }

            // Update in-memory state
            assert logEntry.getIndex() == this.raft.lastAppliedIndex + 1;
            this.raft.incrementLastAppliedIndex(logEntry.getTerm());
            logEntry.applyConfigChange(this.raft.lastAppliedConfig);
            assert this.raft.currentConfig.equals(this.raft.buildCurrentConfig());

            // Delete the log entry
            this.raft.raftLog.remove(0);
            Util.delete(logEntry.getFile(), "applied log file");
        }
    }

    /**
     * Determine whether the given log entry may be applied to the state machine.
     * This method can assume that the log entry is already committed.
     *
     * @param logEntry log entry to apply
     */
    final boolean mayApplyLogEntry(LogEntry logEntry) {
        assert Thread.holdsLock(this.raft);

        // Are we running out of memory, or keeping around too many log entries? If so, go ahead no matter what the subclass says.
        final long logEntryMemoryUsage = this.raft.getUnappliedLogMemoryUsage();
        if (logEntryMemoryUsage > this.raft.maxUnappliedLogMemory || this.raft.raftLog.size() > this.raft.maxUnappliedLogEntries) {
            if (this.log.isTraceEnabled()) {
                this.trace("allowing log entry " + logEntry + " to be applied because memory usage "
                  + logEntryMemoryUsage + " > " + this.raft.maxUnappliedLogMemory + " and/or log length "
                  + this.raft.raftLog.size() + " > " + this.raft.maxUnappliedLogEntries);
            }
            return true;
        }

        // Check with subclass
        return this.roleMayApplyLogEntry(logEntry);
    }

    /**
     * Role-specific hook to determine whether the given log entry should be applied to the state machine.
     * This method can assume that the log entry is already committed.
     *
     * @param logEntry log entry to apply
     */
    boolean roleMayApplyLogEntry(LogEntry logEntry) {
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
        assert this.raft.commitIndex >= this.raft.lastAppliedIndex;
        assert this.raft.commitIndex <= this.raft.lastAppliedIndex + this.raft.raftLog.size();
        assert this.raft.keyWatchIndex <= this.raft.commitIndex;

        // If nobody is watching, don't bother
        if (this.raft.keyWatchTracker == null)
            return;

        // If we have recevied a snapshot install, we may not be able to tell which keys have changed since last notification;
        // in that case, trigger all key watches; otherwise, trigger the keys affected by newly committed log entries
        if (this.raft.keyWatchIndex < this.raft.lastAppliedIndex) {
            this.raft.keyWatchTracker.triggerAll();
            this.raft.keyWatchIndex = this.raft.commitIndex;
        } else {
            while (this.raft.keyWatchIndex < this.raft.commitIndex)
                this.raft.keyWatchTracker.trigger(this.raft.getLogEntryAtIndex(++this.raft.keyWatchIndex).getWrites());
        }
    }

// Transactions

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
    void checkReadyTransaction(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getState().equals(TxState.COMMIT_READY);

        // Determine whether transaction is truly read-only, i.e., it does not imply adding a new Raft log entry
        final boolean readOnly = !tx.addsLogEntry();

        // Check whether we can commit the transaction immediately
        if (readOnly && !tx.getConsistency().isWaitsForLogEntryToBeCommitted()) {           // i.e., UNCOMMITTED, EVENTUAL_COMMITTED
            if (this.log.isTraceEnabled())
                this.trace("trivial commit for read-only, " + tx.getConsistency() + " " + tx);
            this.raft.succeed(tx);
            return;
        }

        // Check whether we don't need to bother talking to the leader
        if (readOnly && !tx.getConsistency().isGuaranteesUpToDateReads()) {                 // i.e., EVENTUAL
            this.advanceReadyTransaction(tx, tx.baseTerm, tx.baseIndex);
            return;
        }

        // Requires leader communication - let subclass handle it
        this.checkReadyLinearizableTransaction(tx, readOnly);                               // i.e., LINEARIZABLE
    }

    /**
     * Check a transaction that is ready to be committed (in the {@link TxState#COMMIT_READY} state)
     * and requires communication with the leader.
     *
     * <p>
     * This will not be invoked unless the transaction is read/write or the consistency level provides up-to-date reads.
     *
     * @param tx the transaction
     * @param readOnly if transaction commit does not imply adding a Raft log entry
     * @throws KVTransactionException if an error occurs
     */
    abstract void checkReadyLinearizableTransaction(RaftKVTransaction tx, boolean readOnly);

    /**
     * Advance a transaction from the {@link TxState#COMMIT_READY} state to the {@link TxState#COMMIT_WAITING} state.
     *
     * @param tx the transaction
     * @param commitTerm term of log entry that must be committed before the transaction may succeed
     * @param commitIndex index of log entry that must be committed before the transaction may succeed
     */
    void advanceReadyTransaction(RaftKVTransaction tx, long commitTerm, long commitIndex) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getState().equals(TxState.COMMIT_READY);

        // Set commit term & index and update state
        if (this.log.isDebugEnabled())
            this.debug("advancing " + tx + " to " + TxState.COMMIT_WAITING + " with commit " + commitIndex + "t" + commitTerm);
        tx.setCommitTerm(commitTerm);
        tx.setCommitIndex(commitIndex);
        tx.setState(TxState.COMMIT_WAITING);

        // Discard information we no longer need
        tx.view.disableReadTracking();

        // Check this transaction to see if it can be committed
        this.raft.requestService(this.checkWaitingTransactionsService);
    }

    /**
     * Check a transaction waiting for its log entry to be committed (in the {@link TxState#COMMIT_WAITING} state).
     *
     * <p>
     * This should be invoked:
     * <ul>
     *  <li>After changing roles</li>
     *  <li>After a transaction has entered the {@link TxState#COMMIT_WAITING} state</li>
     *  <li>After advancing my {@code commitIndex} (as leader or follower)</li>
     *  <li>After receiving an updated {@linkplain AppendResponse#getLeaderLeaseTimeout leader lease timeout}
     *      (in {@link FollowerRole})</li>
     * </ul>
     *
     * @param tx the transaction
     * @throws KVTransactionException if an error occurs
     */
    void checkWaitingTransaction(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getConsistency().isGuaranteesUpToDateReads();

        // Check whether the transaction's commit index and term corresponds to a committed log entry
        final long commitIndex = tx.getCommitIndex();
        final long commitTerm = tx.getCommitTerm();
        if (!tx.isCommitIndexCommitted()) {

            // Has the transaction's commit log entry been committed yet? If not, the transaction is not committed yet.
            if (commitIndex > this.raft.commitIndex)
                return;

            // Determine the actual term of the log entry corresponding to commitIndex
            final long commitIndexActualTerm;
            if (commitIndex >= this.raft.lastAppliedIndex)
                commitIndexActualTerm = this.raft.getLogTermAtIndex(commitIndex);
            else {

                // The commit log entry has already been applied to the state machine; we may or may not know what its term was
                if ((commitIndexActualTerm = this.raft.getAppliedLogEntryTerm(commitIndex)) == 0) {

                    // This can happen if we lose contact and by the time we're back the log entry has
                    // already been applied to the state machine on some leader and that leader sent
                    // us an InstallSnapshot message. We don't know whether it actually got committed
                    // or not, so the transaction must be retried.
                    throw new RetryTransactionException(tx, "commit index " + commitIndex
                      + " < last applied log index " + this.raft.lastAppliedIndex);
                }
            }

            // Verify the term of the committed log entry; if not what we expect, the log entry was overwritten by a new leader
            if (commitTerm != commitIndexActualTerm) {
                throw new RetryTransactionException(tx, "leader was deposed during commit and transaction's log entry "
                  + commitIndex + "t" + commitTerm + " overwritten by " + commitIndex + "t" + commitIndexActualTerm);
            }

            // The transaction's commit log entry is committed
            tx.setCommitIndexCommitted();
        }

        // Check with subclass
        if (!this.mayCommit(tx))
            return;

        // Transaction is officially committed now
        if (this.log.isTraceEnabled())
            this.trace("commit successful for " + tx + " (commit index " + this.raft.commitIndex + " >= " + commitIndex + ")");
        this.raft.succeed(tx);
    }

    boolean mayCommit(RaftKVTransaction tx) {
        return true;
    }

    /**
     * Rebase all rebasable transactions.
     *
     * <p>
     * This should be invoked after appending a new Raft log entry.
     *
     * @param tx the transaction
     * @throws KVTransactionException if an error occurs
     */
    void rebaseTransactions() {

        // Sanity check
        assert Thread.holdsLock(this.raft);

        // Rebase all rebasable transactions
        new ArrayList<>(this.raft.openTransactions.values()).stream()
          .filter(this::shouldRebase)
          .forEach(tx -> {
            try {
                this.rebaseTransaction(tx);
            } catch (KVTransactionException e) {
                this.raft.fail(tx, e);
            } catch (Exception | Error e) {
                this.raft.error("error rebasing transaction " + tx, e);
                this.raft.fail(tx, new KVTransactionException(tx, e));
            }
        });
    }

    /**
     * Rebase the given transaction so that its base log entry the last log entry.
     *
     * <p>
     * This should be invoked for any {@linkplain #shouldRebase rebasable} transaction
     * after appending a new Raft log entry.
     *
     * <p>
     * This method assumes that the given transaction is {@linkplain #shouldRebase rebasable}.
     *
     * @param tx the transaction
     * @throws KVTransactionException if an error occurs
     */
    private void rebaseTransaction(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert this.shouldRebase(tx);
        assert tx.failure == null;
        assert tx.baseIndex >= this.raft.lastAppliedIndex;

        // Lock the mutable view so the rebase appears to happen atomically to any threads viewing the transaction
        synchronized (tx.view) {

            // Check for conflicts between transaction reads and newly committed log entries
            long baseIndex = tx.baseIndex;
            final long lastIndex = this.raft.getLastLogIndex();
            while (baseIndex < lastIndex) {

                // Get log entry
                final LogEntry logEntry = this.raft.getLogEntryAtIndex(++baseIndex);

                // Check for conflict
                if (tx.view.getReads().isConflict(logEntry.getWrites())) {
                    if (this.log.isDebugEnabled())
                        this.debug("cannot rebase " + tx + " past " + logEntry + " due to conflicts, failing");
                    if (this.raft.dumpConflicts)
                        this.dumpConflicts(tx.view.getReads(), logEntry, "local txId=" + tx.txId);
                    throw new RetryTransactionException(tx, "writes of committed transaction at index " + baseIndex
                      + " conflict with transaction reads from transaction base index " + tx.baseIndex);
                }
            }
            assert baseIndex == lastIndex;
            final long baseTerm = this.raft.getLogTermAtIndex(baseIndex);

            // Rebase transaction
            if (this.log.isDebugEnabled())
                this.debug("rebasing " + tx + " from " + tx.baseIndex + "t" + tx.baseTerm + " -> " + baseIndex + "t" + baseTerm);
            switch (tx.getState()) {
            case EXECUTING:
                final MostRecentView view = new MostRecentView(this.raft, false);
                assert view.getIndex() == baseIndex;
                assert view.getTerm() == baseTerm;
                tx.rebase(baseTerm, baseIndex, view.getView().getKVStore(), view.getSnapshot());
                break;
            case COMMIT_READY:
                tx.rebase(baseTerm, baseIndex);
                break;
            default:
                throw new RuntimeException("internal error");
            }
        }
    }

    void dumpConflicts(Reads reads, LogEntry logEntry, String description) {
        final StringBuilder buf = new StringBuilder();
        buf.append(description + " failing due to conflicts with " + logEntry + ":");
        for (String conflict : reads.getConflicts(logEntry.getWrites()))
            buf.append("\n  ").append(conflict);
        this.info(buf.toString());
    }

    /**
     * Determine if the given transaction should be kept rebased on the last log entry.
     */
    boolean shouldRebase(RaftKVTransaction tx) {
        assert Thread.holdsLock(this.raft);
        return (tx.getState().equals(TxState.EXECUTING) || tx.getState().equals(TxState.COMMIT_READY))
          && (tx.getConsistency().isGuaranteesUpToDateReads() || tx.addsLogEntry());
    }

    /**
     * Perform any role-specific transaction cleanups.
     *
     * <p>
     * Invoked either when transaction is completed or this role is being shutdown.
     *
     * <p>
     * The implementation in {@link Role} does nothing; subclasses should override if appropriate.
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
        this.warn("rec'd unexpected message " + msg + " while in role " + this + "; ignoring");
    }

// Debug

    abstract boolean checkState();

    void checkTransaction(RaftKVTransaction tx) {
        if (this.shouldRebase(tx))
            assert tx.baseIndex == this.raft.getLastLogIndex() && tx.baseTerm == this.raft.getLastLogTerm();
    }

// Logging

    void trace(String msg, Throwable t) {
        this.raft.trace(msg, t);
    }

    void trace(String msg) {
        this.raft.trace(msg);
    }

    void debug(String msg, Throwable t) {
        this.raft.debug(msg, t);
    }

    void debug(String msg) {
        this.raft.debug(msg);
    }

    void info(String msg, Throwable t) {
        this.raft.info(msg, t);
    }

    void info(String msg) {
        this.raft.info(msg);
    }

    void warn(String msg, Throwable t) {
        this.raft.warn(msg, t);
    }

    void warn(String msg) {
        this.raft.warn(msg);
    }

    void error(String msg, Throwable t) {
        this.raft.error(msg, t);
    }

    void error(String msg) {
        this.raft.error(msg);
    }

// Object

    @Override
    public abstract String toString();

    String toStringPrefix() {
        assert Thread.holdsLock(this.raft);
        return this.getClass().getSimpleName()
          + "[term=" + this.raft.currentTerm
          + ",applied=" + this.raft.lastAppliedIndex + "t" + this.raft.lastAppliedTerm
          + ",commit=" + this.raft.commitIndex
          + ",log=" + this.raft.raftLog
          + "]";
    }
}

