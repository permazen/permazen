
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.rocksdb;

import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.SnapshotKVDatabase;

/**
 * {@link io.permazen.kv.KVDatabase} implementation based on a {@link RocksDBAtomicKVStore}, providing concurrent transactions
 * and linearizable ACID semantics.
 *
 * <p>
 * {@linkplain RocksDBKVTransaction#watchKey Key watches} are supported.
 */
public class RocksDBKVDatabase extends SnapshotKVDatabase {

// Properties

    /**
     * Configure the underlying {@link RocksDBAtomicKVStore} used by this instance. Required property.
     *
     * @param kvstore underlying key/value store
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setKVStore(RocksDBAtomicKVStore kvstore) {
        super.setKVStore(kvstore);
    }

// KVDatabase

    @Override
    public synchronized RocksDBKVTransaction createTransaction() {
        return (RocksDBKVTransaction)super.createTransaction();
    }

// SnapshotKVDatabase

    @Override
    protected RocksDBKVTransaction createSnapshotKVTransaction(MutableView view, long baseVersion) {
        return new RocksDBKVTransaction(this, view, baseVersion);
    }
}

