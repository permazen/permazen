
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.SnapshotKVDatabase;

/**
 * {@link KVDatabase} implementation based on a {@link MVStoreAtomicKVStore}, providing concurrent transactions
 * and linearizable ACID semantics.
 *
 * <p>
 * Note that this implementation does not use {@link org.h2.mvstore.MVStore}'s built-in transaction managagement. Instead,
 * {@link MVStoreKVDatabase} derives all of its transaction handling behavior from its {@link SnapshotKVDatabase} superclass.
 *
 * <p>
 * {@linkplain MVStoreKVTransaction#watchKey Key watches} are supported.
 */
public class MVStoreKVDatabase extends SnapshotKVDatabase {

// Properties

    /**
     * Get the underlying {@link MVStoreAtomicKVStore} used by this instance.
     *
     * @return underlying key/value store
     */
    public MVStoreAtomicKVStore getKVStore() {
        return (MVStoreAtomicKVStore)super.getKVStore();
    }

    /**
     * Configure the underlying {@link MVStoreAtomicKVStore} used by this instance. Required property.
     *
     * @param kvstore underlying key/value store
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setKVStore(MVStoreAtomicKVStore kvstore) {
        super.setKVStore(kvstore);
    }

// KVDatabase

    @Override
    public synchronized MVStoreKVTransaction createTransaction() {
        return (MVStoreKVTransaction)super.createTransaction();
    }

// SnapshotKVDatabase

    @Override
    protected MVStoreKVTransaction createSnapshotKVTransaction(MutableView view, long baseVersion) {
        return new MVStoreKVTransaction(this, view, baseVersion);
    }
}
