
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.concurrent.Future;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.util.ForwardingKVStore;

/**
 * A dummy {@link KVTransaction} implementation wrapping a provided {@link org.jsimpledb.kv.KVStore}.
 *
 * <p>
 * Instances serve simply to provide access to the underlying {@link org.jsimpledb.kv.KVStore} via the
 * {@link KVTransaction} interface. They cannot be committed or rolled back: all {@link org.jsimpledb.kv.KVStore}
 * methods are supported, but all {@link KVTransaction} methods throw {@link UnsupportedOperationException}.
 */
class SnapshotKVTransaction extends ForwardingKVStore implements KVTransaction {

    private final KVStore kvstore;

    /**
     * Constructor.
     *
     * @param kvstore underlying key/value store
     */
    SnapshotKVTransaction(KVStore kvstore) {
        assert kvstore != null;
        this.kvstore = kvstore;
    }

    @Override
    protected KVStore delegate() {
        return this.kvstore;
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
    public Future<Void> watchKey(byte[] key) {
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

    /**
     * Not supported by {@link SnapshotKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public CloseableKVStore mutableSnapshot() {
        throw new UnsupportedOperationException("snapshot transaction");
    }
}

