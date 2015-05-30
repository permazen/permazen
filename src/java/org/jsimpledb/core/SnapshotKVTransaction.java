
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.util.NavigableMapKVStore;

/**
 * A dummy {@link KVTransaction} implementation based on an underlying {@link org.jsimpledb.kv.KVStore} instead of a
 * {@link org.jsimpledb.kv.KVDatabase}.
 *
 * <p>
 * Instances serve simply to hold state in memory indefinitely. They cannot be committed or rolled back: all
 * {@link org.jsimpledb.kv.KVStore} methods are supported but all {@link KVTransaction} methods throw
 * {@link UnsupportedOperationException}.
 * </p>
 */
class SnapshotKVTransaction extends NavigableMapKVStore implements KVTransaction {

    SnapshotKVTransaction(Transaction tx) {
        tx.db.copyMetaData(tx, this);
    }

    /**
     * Not supported by {@link SnapshotKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public KVDatabase getKVDatabase() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    /**
     * Not supported by {@link SnapshotKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setTimeout(long timeout) {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    /**
     * Not supported by {@link SnapshotKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    /**
     * Not supported by {@link SnapshotKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rollback() {
        throw new UnsupportedOperationException("snapshot transaction");
    }
}

