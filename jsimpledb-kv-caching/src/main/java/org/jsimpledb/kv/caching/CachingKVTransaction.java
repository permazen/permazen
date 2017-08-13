
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.caching;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.util.CloseableIterator;

/**
 * A transaction associated with a {@link CachingKVDatabase}.
 */
public class CachingKVTransaction extends AbstractCachingConfig implements KVTransaction, CloseableKVStore {

    private final CachingKVDatabase kvdb;
    private final KVTransaction inner;
    private final CachingKVStore cachingKV;
    private final MutableView view;

    CachingKVTransaction(CachingKVDatabase kvdb, KVTransaction inner, ExecutorService executor, int rttEstimate) {
        this.kvdb = kvdb;
        this.inner = inner;
        this.cachingKV = new CachingKVStore(inner, executor, rttEstimate);
        this.kvdb.copyCachingConfigTo(this.cachingKV);
        this.view = new MutableView(this.cachingKV);
        this.view.disableReadTracking();
    }

    /**
     * Get the underlying {@link KVTransaction}.
     *
     * @return the wrapped {@link KVTransaction}
     */
    public KVTransaction getInnerTransaction() {
        return this.inner;
    }

    /**
     * Get the underlying {@link CachingKVStore} utilized by this instance.
     *
     * @return the internal {@link CachingKVStore}
     */
    public CachingKVStore getCachingKVStore() {
        return this.cachingKV;
    }

// Closeable

    @Override
    public void close() {
        this.cachingKV.close();
        this.inner.rollback();
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        return this.view.get(key);
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        return this.view.getAtLeast(minKey, maxKey);
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        return this.view.getAtMost(maxKey, minKey);
    }

    @Override
    public CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        return this.view.getRange(minKey, maxKey, reverse);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        this.view.put(key, value);
    }

    @Override
    public void remove(byte[] key) {
        this.view.remove(key);
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        this.view.removeRange(minKey, maxKey);
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        this.view.adjustCounter(key, amount);
    }

    @Override
    public byte[] encodeCounter(long value) {
        return this.view.encodeCounter(value);
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        return this.view.decodeCounter(bytes);
    }

// KVTransaction

    @Override
    public CachingKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    @Override
    public void setTimeout(long timeout) {
        this.inner.setTimeout(timeout);
    }

    @Override
    public boolean isReadOnly() {
        return this.inner.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.inner.setReadOnly(readOnly);
    }

    @Override
    public Future<Void> watchKey(byte[] key) {
        return this.inner.watchKey(key);
    }

    @Override
    public void commit() {
        try {
            if (!this.inner.isReadOnly())
                this.view.getWrites().applyTo(this.inner);
            this.inner.commit();
        } finally {
            this.close();
        }
    }

    @Override
    public void rollback() {
        try {
            this.inner.rollback();
        } finally {
            this.close();
        }
    }

    @Override
    public CloseableKVStore mutableSnapshot() {
        return this.inner.mutableSnapshot();
    }
}
