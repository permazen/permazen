
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.SnapshotKVTransaction;

/**
 * {@link MVStoreKVDatabase} transaction.
 */
public class MVStoreKVTransaction extends SnapshotKVTransaction {

    /**
     * Constructor.
     */
    MVStoreKVTransaction(MVStoreKVDatabase kvdb, MutableView view, long baseVersion) {
        super(kvdb, view, baseVersion);
    }

// KVTransaction

    @Override
    public MVStoreKVDatabase getKVDatabase() {
        return (MVStoreKVDatabase)super.getKVDatabase();
    }
}
