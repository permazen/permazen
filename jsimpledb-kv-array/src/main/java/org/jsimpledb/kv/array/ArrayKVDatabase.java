
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.SnapshotKVDatabase;
import org.jsimpledb.kv.mvcc.SnapshotKVTransaction;

/**
 * {@link org.jsimpledb.kv.KVDatabase} implementation based on a {@link AtomicArrayKVStore}, providing concurrent transactions
 * and linearizable ACID semantics.
 *
 * <p>
 * {@linkplain ArrayKVTransaction#watchKey Key watches},
 * {@linkplain org.jsimpledb.kv.KVTransaction#mutableSnapshot mutable snapshots},
 * and {@linkplain AtomicArrayKVStore#hotCopy hot backups} are supported.
 *
 * @see AtomicArrayKVStore
 */
public class ArrayKVDatabase extends SnapshotKVDatabase {

// Properties

    /**
     * Configure the underlying {@link AtomicArrayKVStore} used by this instance. Required property.
     *
     * @param kvstore underlying key/value store
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setKVStore(AtomicArrayKVStore kvstore) {
        super.setKVStore(kvstore);
    }

// KVDatabase

    @Override
    public synchronized ArrayKVTransaction createTransaction() {
        return (ArrayKVTransaction)super.createTransaction();
    }

// SnapshotKVDatabase

    @Override
    protected ArrayKVTransaction createSnapshotKVTransaction(MutableView view, long baseVersion) {
        return new ArrayKVTransaction(this, view, baseVersion);
    }

    @Override
    protected RuntimeException wrapException(SnapshotKVTransaction tx, RuntimeException e) {
        if (e instanceof ArrayKVException)
            return new KVTransactionException(tx, e.getMessage(), e);
        return e;
    }
}

