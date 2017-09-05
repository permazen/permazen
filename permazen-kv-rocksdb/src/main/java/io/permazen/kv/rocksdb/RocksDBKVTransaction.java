
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.rocksdb;

import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.SnapshotKVTransaction;

/**
 * {@link RocksDBKVDatabase} transaction.
 */
public class RocksDBKVTransaction extends SnapshotKVTransaction {

    /**
     * Constructor.
     */
    RocksDBKVTransaction(RocksDBKVDatabase kvdb, MutableView view, long baseVersion) {
        super(kvdb, view, baseVersion);
    }

// KVTransaction

    @Override
    public RocksDBKVDatabase getKVDatabase() {
        return (RocksDBKVDatabase)super.getKVDatabase();
    }
}

