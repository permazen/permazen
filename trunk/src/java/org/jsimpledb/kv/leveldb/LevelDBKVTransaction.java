
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.leveldb;

import org.jsimpledb.kv.mvcc.SnapshotKVTransaction;
import org.jsimpledb.kv.mvcc.SnapshotVersion;

/**
 * {@link LevelDBKVDatabase} transaction.
 */
public class LevelDBKVTransaction extends SnapshotKVTransaction {

    /**
     * Constructor.
     */
    LevelDBKVTransaction(LevelDBKVDatabase kvdb, SnapshotVersion versionInfo) {
        super(kvdb, versionInfo);
    }

// KVTransaction

    @Override
    public LevelDBKVDatabase getKVDatabase() {
        return (LevelDBKVDatabase)super.getKVDatabase();
    }
}

