
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.util.CloseableForwardingKVStore;
import io.permazen.kv.util.KeyWatchTracker;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableRefs;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KVDatabase} implementation based on an underlying {@link AtomicKVStore} using
 * {@linkplain AtomicKVStore#readOnlySnapshot snapshot} views and optimistic locking to provide concurrent
 * transactions and linearizable ACID consistency.
 *
 * <p>
 * Instances implement a simple optimistic locking scheme for MVCC using {@link AtomicKVStore#readOnlySnapshot}. Concurrent
 * transactions do not contend for any locks until commit time. During each transaction, reads are noted and derive from the
 * snapshot, while writes are batched up. At commit time, if any other transaction has committed writes since the transaction's
 * snapshot was created, and any of those writes {@linkplain Reads#isConflict conflict} with any of the committing
 * transaction's reads, a {@link RetryKVTransactionException} is thrown. Otherwise, the transaction is committed and its
 * writes are applied.
 *
 * <p>
 * Each outstanding transaction's mutations are batched up in memory using a {@link Writes} instance. Therefore,
 * the transaction load supported by this class is limited to what can fit in memory.
 *
 * <p>
 * {@linkplain SnapshotKVTransaction#watchKey Key watches} are supported.
 */
@ThreadSafe
public abstract class SnapshotKVDatabase implements KVDatabase {

// Locking order: (1) SnapshotKVTransaction, (2) SnapshotKVDatabase, (3) MutableView

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

/*

   Open transactions (only) are contained in this.transactions; this.snapshot is the read-only view
   of the underlying key/value store on which all of these open transactions are based. Each transaction has its
   own MutableView of this.snapshot.

   this.snapshot has one reference for being non-null; this reference is shared by all open transactions (if any).
   It also has one reference for each readOnlySnapshot() based on it (see createReadOnlySnapshot()); these references
   are the responsibility of whoever called readOnlySnapshot().

   When a transaction is committed, the mutations are applied to the key/value store and this.snapshot is discarded
   and replaced with a new snapshot of the key/value store, and the MutableView's associated with all other open
   (and non-conflicting) transactions are updated with the new snapshot.

*/

    @GuardedBy("this")
    private final HashSet<SnapshotKVTransaction> transactions = new HashSet<>();
    @GuardedBy("this")
    private CloseableRefs<CloseableKVStore> snapshot;                   // created on-demand for each new version

    @GuardedBy("this")
    private AtomicKVStore kvstore;
    @GuardedBy("this")
    private KeyWatchTracker keyWatchTracker;
    @GuardedBy("this")
    private long currentVersion;
    @GuardedBy("this")
    private boolean started;
    @GuardedBy("this")
    private boolean stopping;

// Constructors

    /**
     * Default constructor.
     *
     * <p>
     * The underlying key/value store must still be configured before starting this instance.
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
     * Required property; must be configured before {@link #start}ing.
     *
     * @param kvstore underlying key/value store
     * @throws IllegalStateException if this instance is already started
     */
    protected synchronized void setKVStore(AtomicKVStore kvstore) {
        Preconditions.checkState(!this.started, "already started");
        this.kvstore = kvstore;
    }

    /**
     * Get the current MVCC version number.
     *
     * @return MVCC database version number
     */
    public synchronized long getCurrentVersion() {
        return this.currentVersion;
    }

// KVDatabase

    @Override
    @PostConstruct
    public synchronized void start() {
        if (this.started)
            return;
        Preconditions.checkState(this.kvstore != null, "no KVStore configured");
        this.kvstore.start();
        this.started = true;
    }

    @Override
    @PreDestroy
    public void stop() {

        // Set stopping flag to prevent new transactions from being created
        synchronized (this) {
            if (!this.started || this.stopping)
                return;
            this.log.info("stopping {}", this);
            this.stopping = true;
        }

        // Close any remaining open transactions, while not holding lock
        this.closeTransactions();

        // Finish up
        synchronized (this) {
            assert this.started;
            if (this.snapshot != null) {
                this.snapshot.unref();
                this.snapshot = null;
            }
            this.kvstore.stop();
            if (this.keyWatchTracker != null) {
                this.keyWatchTracker.close();
                this.keyWatchTracker = null;
            }
            this.stopping = false;
            this.started = false;
        }
    }

    @Override
    public SnapshotKVTransaction createTransaction(Map<String, ?> options) {
        return this.createTransaction();                                            // no options supported yet
    }

    /**
     * Create a new transaction.
     *
     * @throws IllegalStateException if not {@link #start}ed or {@link #stop}ing
     */
    @Override
    public synchronized SnapshotKVTransaction createTransaction() {

        // Sanity check
        Preconditions.checkState(this.started, "not started");
        Preconditions.checkState(!this.stopping, "stopping");

        // Create new transaction
        final MutableView view = new MutableView(this.getCurrentReadOnlySnapshotRefs().getTarget());
        final SnapshotKVTransaction tx = this.createSnapshotKVTransaction(view, this.currentVersion);
        assert !this.transactions.contains(tx);
        this.transactions.add(tx);
        if (this.log.isTraceEnabled())
            this.log.trace("created new transaction {} (new total {})", tx, this.transactions.size());

        // Done
        return tx;
    }

// Key Watches

    synchronized ListenableFuture<Void> watchKey(ByteData key) {
        Preconditions.checkState(this.started, "not started");
        if (this.keyWatchTracker == null)
            this.keyWatchTracker = new KeyWatchTracker();
        return this.keyWatchTracker.register(key);
    }

// Object

    @Override
    public synchronized String toString() {
        return this.getClass().getSimpleName()
          + "[kvstore=" + this.kvstore
          + ",started=" + this.started
          + ",currentVersion=" + this.currentVersion
          + "]";
    }

// Subclass methods

    /**
     * Instantiate a new {@link SnapshotKVTransaction} instance.
     *
     * <p>
     * The implementation in {@link SnapshotKVDatabase} just invokes the {@link SnapshotKVTransaction}
     * constructor using {@code this}. Subclasses may want to override this method to create a more specific subclass.
     *
     * @param view mutable view to be used for this transaction
     * @param baseVersion the database version associated with {@code base}
     * @return new transaction instance
     * @throws KVTransactionException if an error occurs
     */
    protected SnapshotKVTransaction createSnapshotKVTransaction(MutableView view, long baseVersion) {
        return new SnapshotKVTransaction(this, view, baseVersion);
    }

    /**
     * Forcibly fail all outstanding transactions due to {@link #stop} being invoked.
     *
     * <p>
     * Can be used by subclasses during the shutdown sequence to ensure everything is properly cleaned up.
     */
    protected synchronized void closeTransactions() {
        for (SnapshotKVTransaction tx : new ArrayList<>(this.transactions)) {
            if (tx.error == null)
                tx.error = new KVTransactionException(tx, "database was stopped");
            this.cleanupTransaction(tx);
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
            this.log.debug("throwing exception for {}: {}", e.getTransaction(), e.toString());
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
    synchronized void commit(SnapshotKVTransaction tx, boolean readOnly) {
        assert Thread.holdsLock(tx);
        try {
            this.doCommit(tx, readOnly);
        } finally {
            tx.error = null;                                // from this point on, throw a StaleKVTransactionException if accessed
            this.cleanupTransaction(tx);
        }
    }

    /**
     * Rollback a transaction.
     */
    synchronized void rollback(SnapshotKVTransaction tx) {
        assert Thread.holdsLock(tx);
        if (this.log.isTraceEnabled())
            this.log.trace("rolling back transaction {}", tx);
        tx.error = null;                                    // from this point on, throw a StaleKVTransactionException if accessed
        this.cleanupTransaction(tx);
    }

// SnapshotKVTransaction Methods

    synchronized CloseableKVStore createReadOnlySnapshot(Writes writes) {
        final CloseableRefs<CloseableKVStore> snapshotRefs = this.getCurrentReadOnlySnapshotRefs();
        snapshotRefs.ref();
        final MutableView view = new MutableView(snapshotRefs.getTarget(), writes);
        return new CloseableForwardingKVStore(view, snapshotRefs::unref);
    }

// Internal methods

    private synchronized void doCommit(SnapshotKVTransaction tx, boolean readOnly) {

        // Sanity checks
        assert Thread.holdsLock(tx);
        assert Thread.holdsLock(this);

        // Debug
        if (this.log.isTraceEnabled()) {
            this.log.trace("committing transaction {} based on version {} (current version is {})",
              tx, tx.baseVersion, this.currentVersion);
        }

        // Remove transaction; if not there, it's already been invalidated
        if (!this.transactions.remove(tx)) {
            tx.throwErrorIfAny();
            throw this.logException(new StaleKVTransactionException(tx));
        }
        assert tx.error == null;
        assert this.snapshot != null;

        // Grab transaction reads & writes, set to immutable
        final Writes txWrites;
        synchronized (tx.view) {
            txWrites = tx.getMutableView().getWrites();
            tx.view.disableReadTracking();
            tx.view.setReadOnly();
        }

        // If transaction is (effectively) read-only, no need to create a new version
        if (readOnly || txWrites.isEmpty()) {
            if (this.log.isTraceEnabled())
                this.log.trace("no mutations in {}, staying at version {}", tx, this.currentVersion);
            return;
        }

        // Apply the transaction's mutations
        if (this.log.isTraceEnabled()) {
            this.log.trace("applying {} mutations and advancing version from {} -> {}",
              tx, this.currentVersion, this.currentVersion + 1);
        }
        this.kvstore.apply(txWrites, true);

        // Discard the obsolete snapshot and advance the database version
        final CloseableRefs<CloseableKVStore> oldSnapshot = this.snapshot;
        this.snapshot = null;
        tx.setCommitVersion(++this.currentVersion);

        // Check concurrent transactions and invalidate any that have conflicts, or rebase them on the new version
        int numTx = this.transactions.size();                                                       // only used for logging
        for (Iterator<SnapshotKVTransaction> i = this.transactions.iterator(); i.hasNext(); ) {
            final SnapshotKVTransaction victim = i.next();
            assert victim.error == null;
            synchronized (victim.view) {

                // Check for conflict
                final Conflict conflict = victim.view.getReads().findConflict(txWrites);
                if (this.log.isTraceEnabled()) {
                    this.log.trace("ordering {} after {} writes in version {} results in {}",
                      victim, tx, this.currentVersion, conflict != null ? conflict : "no conflict");
//                    if (conflict != null)
//                        this.log.trace("conflicts: {}", victim.view.getReads().getConflicts(txWrites));
                }
                if (conflict != null) {

                    // Mark transaction for failure
                    i.remove();
                    victim.error = new RetryKVTransactionException(victim, String.format(
                      "transaction is based on version %d but the transaction committed at version %d contains conflicting writes",
                      victim.baseVersion, this.currentVersion));
                    if (this.log.isTraceEnabled())
                        this.log.trace("removed conflicting transaction {} (new total {})", victim, --numTx);

                    // This looks weird. What it's really doing is ensuring that any subsequent attempt to access the
                    // data in the transaction via iterators that have already been created will "fail fast" and throw the
                    // RetryKVTransactionException created above. This happens because those accesses go through victim.delegate().
                    victim.view.setKVStore(victim);
                    continue;
                }

                // There was no conflict, so we can safely "rebase" this transaction on the new snapshot
                victim.view.setKVStore(this.getCurrentReadOnlySnapshotRefs().getTarget());
            }
        }

        // Close the old snapshot (but only after rebasing remaining transactions)
        oldSnapshot.unref();

        // Notify watches
        if (this.keyWatchTracker != null)
            this.keyWatchTracker.trigger(txWrites);
    }

    private void cleanupTransaction(SnapshotKVTransaction tx) {

        // Debug
        assert Thread.holdsLock(this);
        if (this.log.isTraceEnabled())
            this.log.trace("cleaning up transaction {}", tx);

        // Remove open transaction from version
        if (this.transactions.remove(tx) && this.log.isTraceEnabled())
            this.log.trace("removed transaction {} (new total {})", tx, this.transactions.size());
    }

    // Get current k/v read-only snapshot, creating on demand as needed
    private CloseableRefs<CloseableKVStore> getCurrentReadOnlySnapshotRefs() {
        assert Thread.holdsLock(this);
        if (this.snapshot == null) {
            this.snapshot = new CloseableRefs<>(this.kvstore.readOnlySnapshot());
            if (this.log.isTraceEnabled())
                this.log.trace("created new snapshot for version {}", this.currentVersion);
        }
        return this.snapshot;
    }
}
