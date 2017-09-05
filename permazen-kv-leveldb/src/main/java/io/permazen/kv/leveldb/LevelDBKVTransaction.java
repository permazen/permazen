
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.SnapshotKVTransaction;

/**
 * {@link LevelDBKVDatabase} transaction.
 */
public class LevelDBKVTransaction extends SnapshotKVTransaction {

    /**
     * Constructor.
     */
    LevelDBKVTransaction(LevelDBKVDatabase kvdb, MutableView view, long baseVersion) {
        super(kvdb, view, baseVersion);
    }

// KVTransaction

    @Override
    public LevelDBKVDatabase getKVDatabase() {
        return (LevelDBKVDatabase)super.getKVDatabase();
    }
}

