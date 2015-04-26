
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.mvcc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-version conccurrency control (MVCC) database using snapshot views of an underlying {@link KVStore}.
 *
 * <p>
 * Supports {@link KVDatabase} implementations based on an underlying {@link KVStore} that can
 * (a) provide read-only snapshot views, and (b) apply mutations atomically. Provides for concurrent transactions
 * linearizable ACID semantics.
 * </p>
 *
 * <p>
 * Instances implement a simple optimistic locking scheme for MVCC using read-only snapshots. Concurrent transactions
 * do not contend for any locks until commit time. During each transaction, reads are noted and pull from the snapshot,
 * while writes are batched up. At commit time, if any other transaction has committed writes since the transaction's
 * snapshot was created, and any of those writes {@linkplain Reads#isConflict conflict} with any of the committing
 * transaction's reads, a {@link RetryTransactionException} is thrown. Otherwise, the transaction is committed and its
 * writes are applied.
 * </p>
 *
 * <p>
 * Each outstanding transaction's mutations are batched up in memory using a {@link MutableView}. Therefore, the transaction
 * load supported by this class is limited by what can fit in memory.
 * </p>
 */
public abstract class SnapshotKVDatabase implements KVDatabase {

// Locking order: (1) SnapshotKVTransaction, (2) SnapshotKVDatabase

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TreeMap<Long, SnapshotVersion> versionInfoMap = new TreeMap<>();

    private long currentVersion;

    protected SnapshotKVDatabase() {
    }

// KVDatabase

    /**
     * Create a new transaction.
     */
    @Override
    public synchronized KVTransaction createTransaction() {

        // Get info for the current version
        final SnapshotVersion versionInfo = this.getCurrentSnapshotVersion();

        // Create the new transaction and associate it with the current version
        final SnapshotKVTransaction tx = this.createSnapshotKVTransaction(versionInfo);
        versionInfo.addOpenTransaction(tx);
        if (this.log.isDebugEnabled())
            this.log.debug("created new transaction " + tx);
        if (this.log.isTraceEnabled())
            this.log.trace("updated current version info: " + versionInfo);

        // Done
        return tx;
    }

// Subclass methods

    /**
     * Instantiate a new {@link SnapshotKVTransaction} instance.
     *
     * <p>
     * The implementation in {@link SnapshotKVDatabase} just invokes the {@link SnapshotKVTransaction}
     * constructor using {@code this} and {@code versionInfo}. Subclasses may want to override this method
     * to create a subclass instead.
     *
     * @param versionInfo associated snapshot info
     * @return new transaction instance
     * @throws KVTransactionException if an error occurs
     */
    protected SnapshotKVTransaction createSnapshotKVTransaction(SnapshotVersion versionInfo) {
        return new SnapshotKVTransaction(this, versionInfo);
    }

    /**
     * Apply the mutations associated with the given transaction to the underlying {@link KVStore}.
     *
     * <p>
     * This method must apply the mutations atomically, and must durably persist them before returning.
     *
     * <p>
     * It is guaranteed that there will be no concurrent invocation of {@link #openSnapshot openSnapshot()}
     * when this method is invoked.
     *
     * @param tx the transaction being committed
     * @throws KVTransactionException if an error occurs
     */
    protected abstract void applyMutations(SnapshotKVTransaction tx);

    /**
     * Create a snapshot of the underlying {@link KVStore}.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>The returned view must provide an unchanging "snapshot" view of the underlying {@link KVStore},
     *      as of the most recent invocation of {@link #applyMutations applyMutations()} (it is guaranteed that
     *      there will be no concurrent invocation of {@link #applyMutations applyMutations()} when this method is invoked).
     *  <li>Multiple snapshot views may be opened at the same time; a snapshot view's contents must not be affected
     *      by the opening of new snapshots, or subsequent invocations of {@link #applyMutations applyMutations()}.</li>
     *  <li>No mutations will be made to the returned view; it is treated as read-only</li>
     * </ul>
     *
     * @return up-to-date snapshot view of the underlying {@link KVStore}
     * @throws KVTransactionException if an error occurs
     */
    protected abstract KVStore openSnapshot();

    /**
     * Close a snapshot view previously returned by {@link #openSnapshot}.
     *
     * @param snapshot {@link KVStore} snapshot to close
     */
    protected abstract void closeSnapshot(KVStore snapshot);

    /**
     * Forcibly close all outstanding transactions.
     *
     * <p>
     * Can be used by subclasses during the shutdown sequence to ensure everything is properly cleaned up.
     * To avoid a possible lock order reversal deadlock, this instance should <b>not</b> be locked when invoking this method.
     */
    protected void closeTransactions() {

        // Grab all remaining open transactions
        final ArrayList<SnapshotKVTransaction> openTransactions = new ArrayList<>();
        synchronized (this) {
            for (SnapshotVersion versionInfo : this.versionInfoMap.values())
                openTransactions.addAll(versionInfo.getOpenTransactions());
        }

        // Close them (but not while holding my lock, to avoid lock order reversal)
        for (SnapshotKVTransaction tx : openTransactions) {
            try {
                tx.rollback();
            } catch (Throwable e) {
                this.log.debug("caught exception closing open transaction during shutdown (ignoring)", e);
            }
        }
    }

// Subclass methods

    /**
     * Log specified exception. Used to ensure exceptions are logged at the point they are thrown.
     *
     * @param e exception to log
     * @return e
     */
    protected KVTransactionException logException(KVTransactionException e) {
        if (this.log.isDebugEnabled())
            this.log.debug("throwing exception for " + e.getTransaction() + ": " + e);
        return e;
    }

// Package methods

    /**
     * Commit a transaction.
     */
    synchronized void commit(SnapshotKVTransaction tx) {
        try {
            this.doCommit(tx);
        } finally {
            this.cleanupTransaction(tx);
        }
    }

    /**
     * Rollback a transaction.
     */
    synchronized void rollback(SnapshotKVTransaction tx) {
        if (this.log.isDebugEnabled())
            this.log.debug("rolling back transaction " + tx);
        this.cleanupTransaction(tx);
    }

// Internal methods

    private synchronized void doCommit(SnapshotKVTransaction tx) {

        // Get current and transaction's version info
        final SnapshotVersion currentSnapshotVersion = this.getCurrentSnapshotVersion();
        final SnapshotVersion transactionSnapshotVersion = tx.getSnapshotVersion();
        final long transactionVersion = transactionSnapshotVersion.getVersion();
        assert this.currentVersion - transactionVersion >= 0;
        assert transactionSnapshotVersion.getOpenTransactions().contains(tx);

        // Debug
        if (this.log.isDebugEnabled()) {
            this.log.debug("committing transaction " + tx + " based on version "
              + transactionVersion + " (current version is " + this.currentVersion + ")");
        }

        // Check whether transaction has been forcibly killed somehow
        if (!transactionSnapshotVersion.getOpenTransactions().contains(tx))
            throw this.logException(new RetryTransactionException(tx, "transaction has been forcibly invalidated"));

        // If the current version has advanced past the transaction's version, check for conflicts from intervening commits
        for (long version = transactionVersion; version != this.currentVersion; version++) {
            final SnapshotKVTransaction committedTransaction = this.versionInfoMap.get(version).getCommittedTransaction();
            final boolean conflict = tx.getMutableView().getReads().isConflict(committedTransaction.getMutableView().getWrites());
            if (this.log.isDebugEnabled()) {
                this.log.debug("ordering " + tx + " after " + committedTransaction + " (version " + version + ") results in "
                  + (conflict ? "conflict" : "no conflict"));
                if (this.log.isTraceEnabled()) {
                    this.log.trace("transaction view: {} committed view: {}",
                      tx.getMutableView(), committedTransaction.getMutableView());
                }
            }
            if (conflict) {
                throw this.logException(new RetryTransactionException(tx, "transaction is based on MVCC version "
                  + transactionSnapshotVersion.getVersion() + " but the transaction committed at MVCC version "
                  + version + " contains conflicting writes"));
            }
        }

        // Apply the transaction's mutations
        if (this.log.isDebugEnabled())
            this.log.debug("applying mutations of " + tx + " to SnapshotMVCC database");
        this.applyMutations(tx);

        // Record transaction's mutations for this version; but discard its reads, we no longer need them
        currentSnapshotVersion.setCommittedTransaction(tx);
        tx.getMutableView().disableReadTracking();

        // Advance to the next MVCC version
        if (this.log.isDebugEnabled())
            this.log.debug("updating current version from " + this.currentVersion + " -> " + (this.currentVersion + 1));
        this.currentVersion++;
    }

    private void cleanupTransaction(SnapshotKVTransaction tx) {

        // Debug
        if (this.log.isTraceEnabled())
            this.log.trace("cleaning up transaction " + tx);

        // Remove open transaction from version
        tx.getSnapshotVersion().removeOpenTransaction(tx);

        // Discard all versions older than all remaining open transactions
        for (Iterator<Map.Entry<Long, SnapshotVersion>> i = this.versionInfoMap.entrySet().iterator(); i.hasNext(); ) {
            final SnapshotVersion versionInfo = i.next().getValue();
            if (!versionInfo.getOpenTransactions().isEmpty())
                break;
            if (this.log.isDebugEnabled()) {
                this.log.debug("discarding obsolete version " + versionInfo + " committed by "
                  + versionInfo.getCommittedTransaction());
            }
            this.closeSnapshot(versionInfo.getSnapshot());
            i.remove();
        }
    }

    // Get SnapshotVersion for the current MVCC version, creating on demand if necessary
    private SnapshotVersion getCurrentSnapshotVersion() {
        SnapshotVersion versionInfo = this.versionInfoMap.get(this.currentVersion);
        if (versionInfo == null) {
            versionInfo = new SnapshotVersion(this.currentVersion, this.openSnapshot());
            this.versionInfoMap.put(this.currentVersion, versionInfo);
            if (this.log.isTraceEnabled())
                this.log.trace("created new version " + versionInfo);
        }
        return versionInfo;
    }
}

