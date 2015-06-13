
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.rocksdb;

import org.jsimpledb.kv.mvcc.SnapshotKVTransaction;
import org.jsimpledb.kv.mvcc.SnapshotVersion;

/**
 * {@link RocksDBKVDatabase} transaction.
 */
public class RocksDBKVTransaction extends SnapshotKVTransaction {

    /**
     * Constructor.
     */
    RocksDBKVTransaction(RocksDBKVDatabase kvdb, SnapshotVersion versionInfo) {
        super(kvdb, versionInfo);
    }

// KVTransaction

    @Override
    public RocksDBKVDatabase getKVDatabase() {
        return (RocksDBKVDatabase)super.getKVDatabase();
    }
}

