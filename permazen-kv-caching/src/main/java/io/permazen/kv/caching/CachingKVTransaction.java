
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.caching;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.mvcc.Writes;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * A transaction associated with a {@link CachingKVDatabase}.
 *
 * <p>
 * Instances create the following "stack":
 * <ul>
 *  <li>A {@link MutableView} to collect any mutations</li>
 *  <li>A {@link CachingKVStore} to cache transaction data</li>
 *  <li>The underlying {@link KVTransaction}</li>
 * </ul>
 */
public class CachingKVTransaction extends AbstractCachingConfig implements KVTransaction, CloseableKVStore {

    /**
     * The associated database.
     */
    protected final CachingKVDatabase kvdb;

    /**
     * The {@link MutableView} that accumulates any mutations.
     */
    protected final MutableView view;

    /**
     * The caching layer for the transaction.
     */
    protected final CachingKVStore cachingKV;

    /**
     * The underlying transaction.
     */
    protected final KVTransaction inner;

    CachingKVTransaction(CachingKVDatabase kvdb, KVTransaction inner, ExecutorService executor, long rttEstimate) {
        this.kvdb = kvdb;
        this.inner = inner;
        this.cachingKV = new CachingKVStore(inner, executor, rttEstimate);
        this.kvdb.copyCachingConfigTo(this.cachingKV);
        this.view = new MutableView(this.cachingKV, false);
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
        this.kvdb.updateRttEstimate(this.cachingKV.getRttEstimate());
        this.cachingKV.close();
        this.inner.rollback();
    }

// KVStore

    @Override
    public ByteData get(ByteData key) {
        return this.view.get(key);
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        return this.view.getAtLeast(minKey, maxKey);
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        return this.view.getAtMost(maxKey, minKey);
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        return this.view.getRange(minKey, maxKey, reverse);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        this.view.put(key, value);
    }

    @Override
    public void remove(ByteData key) {
        this.view.remove(key);
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        this.view.removeRange(minKey, maxKey);
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        this.view.adjustCounter(key, amount);
    }

    @Override
    public ByteData encodeCounter(long value) {
        return this.view.encodeCounter(value);
    }

    @Override
    public long decodeCounter(ByteData bytes) {
        return this.view.decodeCounter(bytes);
    }

    @Override
    public void apply(Mutations mutations) {
        this.view.apply(mutations);
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
    public Future<Void> watchKey(ByteData key) {
        return this.inner.watchKey(key);
    }

    @Override
    public void commit() {

        // Grab transaction reads & writes, set to immutable
        final Writes writes;
        synchronized (this.view) {
            writes = this.view.getWrites();
            this.view.setReadOnly();
            this.cachingKV.close();                 // this tells background read-ahead threads to ignore subsequent exceptions
        }

        // Apply writes and commit tx
        try {
            this.applyWritesBeforeCommitIfNotReadOnly(writes);
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
    public CloseableKVStore readOnlySnapshot() {
        return this.inner.readOnlySnapshot();
    }

// Other methods

    /**
     * Apply accumulated mutations just prior to {@link commit commit()}'ing the transaction.
     *
     * <p>
     * The implementation in {@link CachingKVTransaction} checks whether the inner transaction {@link #isReadOnly},
     * and if so invokes {@link Writes#applyTo Writes.applyTo}.
     *
     * @param writes the mutations to apply
     */
    protected void applyWritesBeforeCommitIfNotReadOnly(Writes writes) {
        if (!this.inner.isReadOnly())
            writes.applyTo(this.inner);
    }
}
