
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.mvcc.Mutations;
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
        for (RaftKVTransaction tx : new ArrayList<RaftKVTransaction>(this.raft.openTransactions.values()))
            new CheckReadyTransactionService(this, tx).run();
    }

    /**
     * Check transactions in the {@link TxState#COMMIT_WAITING} state to see if they are committed yet.
     * We invoke this service method whenever our {@code commitIndex} advances.
     */
    void checkWaitingTransactions() {
        assert Thread.holdsLock(this.raft);
        for (RaftKVTransaction tx : new ArrayList<RaftKVTransaction>(this.raft.openTransactions.values()))
            new CheckWaitingTransactionService(this, tx).run();
    }

    /**
     * Apply committed but unapplied log entries to the state machine.
     * We invoke this service method whenever log entries are added or our {@code commitIndex} advances.
     */
    void applyCommittedLogEntries() {
        assert Thread.holdsLock(this.raft);

        // Apply committed log entries to the state machine
        while (this.raft.lastAppliedIndex < this.raft.commitIndex) {

            // Grab the first unwritten log entry
            final LogEntry logEntry = this.raft.raftLog.get(0);
            assert logEntry.getIndex() == this.raft.lastAppliedIndex + 1;

            // Check with subclass
            if (!this.mayApplyLogEntry(logEntry))
                break;

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

            // Apply updates to the key/value store (durably)
            if (this.log.isDebugEnabled())
                this.debug("applying committed log entry " + logEntry + " to key/value store");
            try {
                this.raft.kv.mutate(mutations, true);
            } catch (Exception e) {
                if (e instanceof RuntimeException && e.getCause() instanceof IOException)
                    e = (IOException)e.getCause();
                this.error("error applying log entry " + logEntry + " to key/value store", e);
                break;
            }

            // Update in-memory state
            this.raft.lastAppliedTerm = logEntry.getTerm();
            assert logEntry.getIndex() == this.raft.lastAppliedIndex + 1;
            this.raft.lastAppliedIndex = logEntry.getIndex();
            logEntry.applyConfigChange(this.raft.lastAppliedConfig);
            assert this.raft.currentConfig.equals(this.raft.buildCurrentConfig());

            // Delete the log entry
            this.raft.raftLog.remove(0);
            Util.delete(logEntry.getFile(), "applied log file");
        }
    }

    /**
     * Determine whether the given log entry may be applied to the state machine.
     */
    boolean mayApplyLogEntry(LogEntry logEntry) {
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

        // Get transaction mutations
        final Writes writes = tx.getMutableView().getWrites();
        final String[] configChange = tx.getConfigChange();

        // Determine whether transaction is truly read-only
        final boolean readOnly = tx.isReadOnly() || (writes.isEmpty() && configChange == null);

        // Check whether we can commit the transaction immediately
        if (readOnly && !tx.getConsistency().isWaitsForLogEntryToBeCommitted()) {           // i.e., UNCOMMITTED
            if (this.log.isTraceEnabled())
                this.trace("trivial commit for read-only, " + tx.getConsistency() + " " + tx);
            this.raft.succeed(tx);
            return;
        }

        // Check whether we don't need to bother talking to the leader
        if (readOnly && !tx.getConsistency().isGuaranteesUpToDateReads()) {                 // i.e., EVENTUAL, EVENTUAL_COMMITTED
            this.advanceReadyTransaction(tx, tx.getBaseTerm(), tx.getBaseIndex());
            return;
        }

        // Requires leader communication - let subclass handle it
        this.checkReadyLeaderTransaction(tx, readOnly);
    }

    /**
     * Check a transaction that is ready to be committed (in the {@link TxState#COMMIT_READY} state)
     * and requires communication with the leader.
     *
     * <p>
     * This will not be invoked unless the transaction is read/write or the consistency level provides up-to-date reads.
     *
     * @param tx the transaction
     * @param readOnly if transaction is read-only
     * @throws KVTransactionException if an error occurs
     */
    abstract void checkReadyLeaderTransaction(RaftKVTransaction tx, boolean readOnly);

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
        tx.getMutableView().disableReadTracking();

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

        // Handle the case the transaction's committed log index has already been applied to the state machine
        final long commitIndex = tx.getCommitIndex();
        if (commitIndex < this.raft.lastAppliedIndex) {

            // This can happen if we lose contact and by the time we're back the log entry has
            // already been applied to the state machine on some leader and that leader sent
            // use an InstallSnapshot message. We don't know whether it actually got committed
            // or not, so the transaction must be retried.
            throw new RetryTransactionException(tx, "committed log entry was missed");
        }

        // Has the transaction's log entry been received and committed yet?
        if (commitIndex > this.raft.commitIndex)
            return;

        // Verify the term of the committed log entry; if not what we expect, the log entry was overwritten by a new leader
        final long commitTerm = tx.getCommitTerm();
        if (this.raft.getLogTermAtIndex(commitIndex) != commitTerm)
            throw new RetryTransactionException(tx, "leader was deposed during commit and transaction's log entry overwritten");

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
     * Perform any role-specific transaction cleanups.
     *
     * <p>
     * Invoked either when transaction is closed or this role is being shutdown.
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
    abstract void caseAppendRequest(AppendRequest msg);
    abstract void caseAppendResponse(AppendResponse msg);
    abstract void caseCommitRequest(CommitRequest msg);
    abstract void caseCommitResponse(CommitResponse msg);
    abstract void caseGrantVote(GrantVote msg);
    abstract void caseInstallSnapshot(InstallSnapshot msg);
    abstract void caseRequestVote(RequestVote msg);

    void casePingRequest(PingRequest msg) {
        assert Thread.holdsLock(this.raft);
        this.raft.sendMessage(new PingResponse(this.raft.clusterId,
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

    boolean checkState() {
        return true;
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

