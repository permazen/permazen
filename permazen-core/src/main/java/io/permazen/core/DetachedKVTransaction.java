
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.util.ByteData;

import java.util.concurrent.Future;

/**
 * A dummy {@link KVTransaction} implementation wrapping a provided {@link KVStore}.
 *
 * <p>
 * Instances serve simply to provide access to the underlying {@link KVStore} via the
 * {@link KVTransaction} interface. They cannot be committed or rolled back: all {@link KVStore}
 * methods are supported, but all {@link KVTransaction} methods throw {@link UnsupportedOperationException}.
 */
class DetachedKVTransaction extends ForwardingKVStore implements KVTransaction {

    private final KVStore kvstore;

    private volatile boolean readOnly;

    /**
     * Constructor.
     *
     * @param kvstore underlying key/value store
     */
    DetachedKVTransaction(KVStore kvstore) {
        assert kvstore != null;
        this.kvstore = kvstore;
    }

    @Override
    protected KVStore delegate() {
        return this.kvstore;
    }

    /**
     * Not supported by {@link DetachedKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public KVDatabase getKVDatabase() {
        throw new UnsupportedOperationException("detached transaction");
    }

    /**
     * Not supported by {@link DetachedKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setTimeout(long timeout) {
        throw new UnsupportedOperationException("detached transaction");
    }

    /**
     * Not supported by {@link DetachedKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Future<Void> watchKey(ByteData key) {
        throw new UnsupportedOperationException("detached transaction");
    }

    @Override
    public boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Not supported by {@link DetachedKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("detached transaction");
    }

    /**
     * Not supported by {@link DetachedKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rollback() {
        throw new UnsupportedOperationException("detached transaction");
    }

    /**
     * Not supported by {@link DetachedKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public CloseableKVStore readOnlySnapshot() {
        throw new UnsupportedOperationException("detached transaction");
    }
}
