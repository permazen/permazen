
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.SnapshotKVTransaction;

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

