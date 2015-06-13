
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.rocksdb;

import org.jsimpledb.kv.mvcc.SnapshotKVDatabase;
import org.jsimpledb.kv.mvcc.SnapshotVersion;

/**
 * {@link org.jsimpledb.kv.KVDatabase} implementation based on a {@link RocksDBAtomicKVStore}, providing concurrent transactions
 * and linearizable ACID semantics.
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
    protected RocksDBKVTransaction createSnapshotKVTransaction(SnapshotVersion versionInfo) {
        return new RocksDBKVTransaction(this, versionInfo);
    }
}

