
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
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
 * Multi-version conccurrency control (MVCC) database using snapshot views of an underlying {@link AtomicKVStore}.
 *
 * <p>
 * This class provides a {@link KVDatabase} view of an underlying {@link AtomicKVStore},
 * supporting concurrent transactions via MVCC and linearizable ACID semantics.
 * </p>
 *
 * <p>
 * Instances implement a simple optimistic locking scheme for MVCC using {@link AtomicKVStore#snapshot}. Concurrent transactions
 * do not contend for any locks until commit time. During each transaction, reads are noted and derive from the snapshot,
 * while writes are batched up. At commit time, if any other transaction has committed writes since the transaction's
 * snapshot was created, and any of those writes {@linkplain Reads#isConflict conflict} with any of the committing
 * transaction's reads, a {@link RetryTransactionException} is thrown. Otherwise, the transaction is committed and its
 * writes are applied.
 * </p>
 *
 * <p>
 * Each outstanding transaction's mutations are batched up in memory using a {@link Writes} instance. Therefore, the
 * transaction load supported by this class is limited to what can fit in memory.
 * </p>
 */
public class SnapshotKVDatabase implements KVDatabase {

// Locking order: (1) SnapshotKVTransaction, (2) SnapshotKVDatabase

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TreeMap<Long, SnapshotVersion> versionInfoMap = new TreeMap<>();

    private AtomicKVStore kvstore;
    private long currentVersion;

// Constructors

    /**
     * Default constructor.
     *
     * <p>
     * The underlying key/value store must still be configured before creating any transactions.
     */
    public SnapshotKVDatabase() {
    }

    /**
     * Constructor.
     *
     * @param kvstore underlying key/value store
     */
    public SnapshotKVDatabase(AtomicKVStore kvstore) {
        this.kvstore = kvstore;
    }

// Properties

    /**
     * Get the underlying {@link AtomicKVStore}.
     *
     * @return underlying key/value store
     */
    protected synchronized AtomicKVStore getKVStore() {
        return this.kvstore;
    }

    /**
     * Configure the underlying {@link AtomicKVStore}.
     *
     * <p>
     * Required property; must be configured before creating any transactions.
     *
     * @param kvstore underlying key/value store
     * @throws IllegalStateException if there are any transactions open
     */
    protected synchronized void setKVStore(AtomicKVStore kvstore) {
        if (!this.versionInfoMap.isEmpty())
            throw new IllegalStateException("transactions exist");
        this.kvstore = kvstore;
    }

// KVDatabase

    /**
     * Create a new transaction.
     *
     * @throws IllegalStateException if no {@link AtomicKVStore} is configured
     */
    @Override
    public synchronized KVTransaction createTransaction() {

        // Sanity check
        if (this.kvstore == null)
            throw new IllegalStateException("no KVSTore configured");

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
     * to create a more specific subclass.
     *
     * @param versionInfo associated snapshot info
     * @return new transaction instance
     * @throws KVTransactionException if an error occurs
     */
    protected SnapshotKVTransaction createSnapshotKVTransaction(SnapshotVersion versionInfo) {
        return new SnapshotKVTransaction(this, versionInfo);
    }

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

    /**
     * Wrap a {@link RuntimeException} as needed.
     *
     * <p>
     * The implementation in {@link SnapshotKVDatabase} just returns {@code e}.
     *
     * @param tx transaction in which the exception occurred
     * @param e original exception
     * @return wrapped exception, or just {@code e}
     */
    protected RuntimeException wrapException(SnapshotKVTransaction tx, RuntimeException e) {
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

        // Get transaction reads & writes
        final Reads transactionReads = tx.getMutableView().getReads();
        final Writes transactionWrites = tx.getMutableView().getWrites();

        // If the current version has advanced past the transaction's version, check for conflicts from intervening commits
        for (long version = transactionVersion; version != this.currentVersion; version++) {
            final SnapshotVersion committedSnapshotVersion = this.versionInfoMap.get(version);
            final Writes committedWrites = committedSnapshotVersion.getCommittedWrites();
            final boolean conflict = transactionReads.isConflict(committedWrites);
            if (this.log.isDebugEnabled()) {
                this.log.debug("ordering " + tx + " after writes in version " + version + " results in "
                  + (conflict ? "conflict" : "no conflict"));
                if (this.log.isTraceEnabled())
                    this.log.trace("transaction reads: {} committed writes: {}", transactionReads, committedWrites);
            }
            if (conflict) {
                throw this.logException(new RetryTransactionException(tx, "transaction is based on MVCC version "
                  + transactionSnapshotVersion.getVersion() + " but the transaction committed at MVCC version "
                  + version + " contains conflicting writes"));
            }
        }

        // Atomically apply the transaction's mutations
        if (this.log.isDebugEnabled())
            this.log.debug("applying mutations of " + tx + " to SnapshotMVCC database");
        this.kvstore.mutate(transactionWrites, true);

        // Record transaction's writes for this version
        currentSnapshotVersion.setCommittedWrites(transactionWrites);

        // Advance to the next MVCC version
        if (this.log.isDebugEnabled())
            this.log.debug("updating current version from " + this.currentVersion + " -> " + (this.currentVersion + 1));
        this.currentVersion++;
    }

    private void cleanupTransaction(SnapshotKVTransaction tx) {

        // Debug
        assert Thread.holdsLock(this);
        if (this.log.isTraceEnabled())
            this.log.trace("cleaning up transaction " + tx);

        // Remove open transaction from version
        tx.getSnapshotVersion().removeOpenTransaction(tx);

        // Discard all versions older than all remaining open transactions
        for (Iterator<Map.Entry<Long, SnapshotVersion>> i = this.versionInfoMap.entrySet().iterator(); i.hasNext(); ) {
            final SnapshotVersion versionInfo = i.next().getValue();
            if (!versionInfo.getOpenTransactions().isEmpty())
                break;
            if (this.log.isDebugEnabled())
                this.log.debug("discarding obsolete version " + versionInfo);
            final KVStore snapshot = versionInfo.getSnapshot();
            if (snapshot instanceof AutoCloseable) {
                try {
                    ((AutoCloseable)snapshot).close();
                } catch (Exception e) {
                    // ignore
                }
            }
            i.remove();
        }
    }

    // Get SnapshotVersion for the current MVCC version, creating on demand if necessary
    private SnapshotVersion getCurrentSnapshotVersion() {
        SnapshotVersion versionInfo = this.versionInfoMap.get(this.currentVersion);
        if (versionInfo == null) {
            versionInfo = new SnapshotVersion(this.currentVersion, this.kvstore.snapshot());
            this.versionInfoMap.put(this.currentVersion, versionInfo);
            if (this.log.isTraceEnabled())
                this.log.trace("created new version " + versionInfo);
        }
        return versionInfo;
    }
}

