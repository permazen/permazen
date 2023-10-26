
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.SnapshotKVDatabase;
import io.permazen.kv.mvcc.SnapshotKVTransaction;

import org.iq80.leveldb.DBException;

/**
 * {@link KVDatabase} implementation based on a {@link LevelDBAtomicKVStore}, providing concurrent transactions
 * and linearizable ACID semantics.
 *
 * <p>
 * {@linkplain LevelDBKVTransaction#watchKey Key watches} are supported.
 */
public class LevelDBKVDatabase extends SnapshotKVDatabase {

// Properties

    /**
     * Get the underlying {@link LevelDBAtomicKVStore} used by this instance.
     *
     * @return underlying key/value store
     */
    public LevelDBAtomicKVStore getKVStore() {
        return (LevelDBAtomicKVStore)super.getKVStore();
    }

    /**
     * Configure the underlying {@link LevelDBAtomicKVStore} used by this instance. Required property.
     *
     * @param kvstore underlying key/value store
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setKVStore(LevelDBAtomicKVStore kvstore) {
        super.setKVStore(kvstore);
    }

// KVDatabase

    @Override
    public synchronized LevelDBKVTransaction createTransaction() {
        return (LevelDBKVTransaction)super.createTransaction();
    }

// SnapshotKVDatabase

    @Override
    protected LevelDBKVTransaction createSnapshotKVTransaction(MutableView view, long baseVersion) {
        return new LevelDBKVTransaction(this, view, baseVersion);
    }

    @Override
    protected RuntimeException wrapException(SnapshotKVTransaction tx, RuntimeException e) {
        if (e instanceof DBException)
            return new KVTransactionException(tx, "LevelDB error", e);
        return e;
    }
}
