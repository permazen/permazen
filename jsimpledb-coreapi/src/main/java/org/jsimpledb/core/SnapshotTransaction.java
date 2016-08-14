
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.jsimpledb.kv.KVStore;

/**
 * A "snapshot" {@link Transaction} that persists indefinitely.
 *
 * <p>
 * {@link SnapshotTransaction}s hold a "snapshot" of some portion of the state of a {@link Transaction}
 * for later use. Each {@link SnapshotTransaction} contains its own set of "snapshot" objects.
 *
 * <p>
 * {@link SnapshotTransaction}s can never be closed (i.e., committed or rolled-back); they persist in memory until
 * no longer referenced. {@link Transaction.Callback}s may be registered but they will never be invoked.
 *
 * <p>
 * {@link SnapshotTransaction}s can be based on an arbitrary {@link KVStore};
 * see {@link Database#createSnapshotTransaction Database.createSnapshotTransaction()}.
 *
 * @see Transaction#createSnapshotTransaction Transaction.createSnapshotTransaction()
 * @see Database#createSnapshotTransaction Database.createSnapshotTransaction()
 * @see org.jsimpledb.SnapshotJTransaction
 */
public class SnapshotTransaction extends Transaction {

// Constructors

    SnapshotTransaction(Database db, KVStore kvstore, Schemas schemas) {
        super(db, new SnapshotKVTransaction(kvstore), schemas);
    }

    SnapshotTransaction(Database db, KVStore kvstore, Schemas schemas, int versionNumber) {
        super(db, new SnapshotKVTransaction(kvstore), schemas, versionNumber);
    }

    SnapshotTransaction(Database db, KVStore kvstore, Schemas schemas, Schema schema) {
        super(db, new SnapshotKVTransaction(kvstore), schemas, schema);
    }

// Methods

    @Override
    public boolean isSnapshot() {
        return true;
    }

    /**
     * Get the underlying {@link KVStore} that holds this snapshot transaction's state.
     *
     * @return underlying {@link KVStore}
     */
    public KVStore getKVStore() {
        return ((SnapshotKVTransaction)this.kvt).delegate();
    }

    /**
     * Delete all objects contained in this snapshot transaction.
     *
     * <p>
     * It will contain schema meta-data but no objects.
     */
    public synchronized void reset() {

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
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rollback() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    /**
     * Register a transaction callback to be invoked when this transaction completes.
     *
     * <p>
     * {@link Transaction.Callback}s registered with a {@link SnapshotTransaction} will by definition never be invoked.
     * Therefore, this method simply discards {@code callback}.
     */
    @Override
    public void addCallback(Callback callback) {
    }

    /**
     * Determine whether this transaction is still valid.
     *
     * <p>
     * {@link SnapshotTransaction}s are always valid.
     *
     * @return true always
     */
    @Override
    public boolean isValid() {
        return super.isValid();
    }
}

