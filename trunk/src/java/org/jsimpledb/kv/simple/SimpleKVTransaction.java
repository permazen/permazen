
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.simple;

import java.util.SortedSet;
import java.util.TreeSet;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.util.CountingKVStoreAdapter;
import org.jsimpledb.kv.util.LockOwner;
import org.jsimpledb.util.ByteUtil;

/**
 * {@link KVTransaction} implementation for {@link SimpleKVDatabase}.
 *
 * <p>
 * Note lock order: first {@link SimpleKVTransaction}, then {@link SimpleKVDatabase}.
 * </p>
 */
class SimpleKVTransaction extends CountingKVStoreAdapter implements KVTransaction {

    final SimpleKVDatabase kvstore;
    final TreeSet<Mutation> mutations = new TreeSet<>(KeyRange.SORT_BY_MIN);
    final LockOwner lockOwner = new LockOwner();

    boolean stale;
    long waitTimeout;

    @SuppressWarnings("unchecked")
    SimpleKVTransaction(SimpleKVDatabase kvstore, long waitTimeout) {
        if (kvstore == null)
            throw new IllegalArgumentException("null kvstore");
        this.kvstore = kvstore;
        this.waitTimeout = waitTimeout;
    }

    @Override
    public KVDatabase getKVDatabase() {
        return this.kvstore;
    }

    @Override
    public void setTimeout(long timeout) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout < 0");
        if (this.stale)
            throw new StaleTransactionException(this);
        this.waitTimeout = timeout;
    }

    @Override
    public synchronized byte[] get(byte[] key) {
        if (this.stale)
            throw new StaleTransactionException(this);
        return this.kvstore.get(this, key);
    }

    @Override
    public synchronized KVPair getAtLeast(byte[] key) {
        if (this.stale)
            throw new StaleTransactionException(this);
        return this.kvstore.getAtLeast(this, key);
    }

    @Override
    public synchronized KVPair getAtMost(byte[] key) {
        if (this.stale)
            throw new StaleTransactionException(this);
        return this.kvstore.getAtMost(this, key);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.kvstore.put(this, key, value);
    }

    @Override
    public synchronized void remove(byte[] key) {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.kvstore.remove(this, key);
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.kvstore.removeRange(this, minKey, maxKey);
    }

    @Override
    public synchronized void commit() {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.stale = true;
        this.kvstore.commit(this);
    }

    @Override
    public synchronized void rollback() {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.stale = true;
        this.kvstore.rollback(this);
    }

    // Find the mutation that overlaps with the given key, if any.
    // This method assumes we are already synchronized.
    Mutation findMutation(byte[] key) {
        final SortedSet<Mutation> left = this.mutations.headSet(Mutation.key(ByteUtil.getNextKey(key)));
        if (!left.isEmpty()) {
            final Mutation last = left.last();
            if (last.contains(key))
                return last;
        }
        return null;
    }
}

