
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.rocksdb;

import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.SnapshotKVTransaction;

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

