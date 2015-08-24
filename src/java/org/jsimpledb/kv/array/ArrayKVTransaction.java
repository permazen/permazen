
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import org.jsimpledb.kv.mvcc.SnapshotKVTransaction;
import org.jsimpledb.kv.mvcc.SnapshotVersion;

/**
 * {@link ArrayKVDatabase} transaction.
 */
public class ArrayKVTransaction extends SnapshotKVTransaction {

    /**
     * Constructor.
     */
    ArrayKVTransaction(ArrayKVDatabase kvdb, SnapshotVersion versionInfo) {
        super(kvdb, versionInfo);
    }

// KVTransaction

    @Override
    public ArrayKVDatabase getKVDatabase() {
        return (ArrayKVDatabase)super.getKVDatabase();
    }
}

