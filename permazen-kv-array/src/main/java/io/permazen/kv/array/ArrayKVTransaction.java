
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.SnapshotKVTransaction;

/**
 * {@link ArrayKVDatabase} transaction.
 */
public class ArrayKVTransaction extends SnapshotKVTransaction {

    /**
     * Constructor.
     */
    ArrayKVTransaction(ArrayKVDatabase kvdb, MutableView view, long baseVersion) {
        super(kvdb, view, baseVersion);
    }

// KVTransaction

    @Override
    public ArrayKVDatabase getKVDatabase() {
        return (ArrayKVDatabase)super.getKVDatabase();
    }
}
