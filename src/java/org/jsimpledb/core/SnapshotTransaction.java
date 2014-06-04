
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * An initially empty, in-memory {@link Transaction} that persists indefinitely.
 *
 * <p>
 * {@link SnapshotTransaction}s hold a "snapshot" some portion of the state of a {@link Transaction} for later use.
 * Each {@link SnapshotTransaction}s contains its own set of "snapshot" objects that are copies of
 * corresponding "real" database objects (as of the time they were copied).
 * </p>
 *
 * <p>
 * {@link SnapshotTransaction}s can never be closed (i.e., committed or rolled-back); they persist in memory until
 * no longer referenced. {@link Transaction.Callback}s may be registered but they will never be invoked.
 * </p>
 *
 * @see Transaction#createSnapshotTransaction
 */
public class SnapshotTransaction extends Transaction {

    SnapshotTransaction(Transaction parent) {
        super(parent.db, new SnapshotKVTransaction(parent), parent.schema, parent.version);
    }

    /**
     * Delete all objects contained in this snapshot transaction and reset it back to its initial empty state.
     *
     * <p>
     * It will contain schema meta-data but no objects.
     * </p>
     */
    public void reset() {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);

        // Delete all object and index keys
        this.db.reset(this);
    }

    /**
     * Commit this transaction.
     *
     * <p>
     * {@link SnapshotTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     * </p>
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    /**
     * Roll back this transaction.
     *
     * <p>
     * {@link SnapshotTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     * </p>
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rollback() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    /**
     * Mark this transaction for rollback only.
     *
     * <p>
     * {@link SnapshotTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     * </p>
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setRollbackOnly() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    /**
     * Register a transaction callback to be invoked when this transaction completes.
     *
     * <p>
     * {@link Transaction.Callback}s registered with a {@link SnapshotTransaction} will by definition never be invoked.
     * Therefore, this method simply discards {@code callback}.
     * </p>
     */
    @Override
    public void addCallback(Callback callback) {
    }

    /**
     * Determine whether this transaction is still valid.
     *
     * <p>
     * {@link SnapshotTransaction}s are always valid.
     * </p>
     *
     * @return true always
     */
    @Override
    public boolean isValid() {
        return super.isValid();
    }
}

