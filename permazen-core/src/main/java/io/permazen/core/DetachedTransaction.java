
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;

import java.io.Closeable;

/**
 * A {@link Transaction} that is not actually connected to a persistent database, but instead exists
 * just to hold objects in memory.
 *
 * <p>
 * {@link DetachedTransaction}s are typically used to hold a "snapshot" some portion of a normal {@link Transaction}
 * for later use. As with all transactions, each {@link DetachedTransaction} contains its own object data.
 *
 * <p>
 * {@link DetachedTransaction}s cannot be committed or rolled-back; they just persist in memory until
 * no longer needed. {@link Transaction.Callback}s may be registered, but they will never be invoked.
 *
 * <p>
 * {@link DetachedTransaction}s can be based on an arbitrary {@link KVStore};
 * see {@link Database#createDetachedTransaction Database.createDetachedTransaction()}.
 *
 + * <p><b>Lifecycle Management</b>
 *
 * <p>
 * Instances of this class should be {@link #close}'d when no longer needed to release any associated resources.
 *
 * @see Transaction#createDetachedTransaction Transaction.createDetachedTransaction()
 * @see Database#createDetachedTransaction Database.createDetachedTransaction()
 * @see io.permazen.DetachedPermazenTransaction
 */
public class DetachedTransaction extends Transaction implements Closeable {

// Constructors

    DetachedTransaction(Database db, KVStore kvstore, Schema schema) {
        super(db, new DetachedKVTransaction(kvstore), schema);
    }

// Methods

    @Override
    public boolean isDetached() {
        return true;
    }

    /**
     * Get the underlying {@link KVStore} that holds this detached transaction's state.
     *
     * @return underlying {@link KVStore}
     */
    public KVStore getKVStore() {
        return ((DetachedKVTransaction)this.kvt).delegate();
    }

    /**
     * Delete all objects contained in this detached transaction.
     *
     * <p>
     * Upon return, the underlying key/value store will still contain meta-data, but no objects.
     */
    public synchronized void reset() {
        if (this.stale)
            throw new StaleTransactionException(this);
        Layout.deleteObjectData(this.kvt);
    }

    /**
     * Commit this transaction.
     *
     * <p>
     * {@link DetachedTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("detached transaction");
    }

    /**
     * Roll back this transaction.
     *
     * <p>
     * {@link DetachedTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rollback() {
        throw new UnsupportedOperationException("detached transaction");
    }

    /**
     * Register a transaction callback to be invoked when this transaction completes.
     *
     * <p>
     * {@link Transaction.Callback}s registered with a {@link DetachedTransaction} will by definition never be invoked.
     * Therefore, this method simply discards {@code callback}.
     */
    @Override
    public void addCallback(Callback callback) {
    }

    /**
     * Determine whether this transaction is still open.
     *
     * <p>
     * {@link DetachedTransaction}s are always open.
     *
     * @return true always
     */
    @Override
    public boolean isOpen() {
        return super.isOpen();
    }

// Closeable

    /**
     * Close this instance and release any resources associated with it.
     *
     * <p>
     * The implementation in {@link DetachedTransaction} closes the underlying {@link KVStore} if it is
     * a {@link CloseableKVStore}, otherwise it does nothing.
     */
    @Override
    public void close() {
        final KVStore kvstore = this.getKVStore();
        if (kvstore instanceof CloseableKVStore)
            ((CloseableKVStore)kvstore).close();
    }
}
