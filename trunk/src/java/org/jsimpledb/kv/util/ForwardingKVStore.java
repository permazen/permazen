
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.Iterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;

/**
 * Forwards all {@link KVStore} operations to another underlying {@link KVStore}.
 */
public class ForwardingKVStore implements KVStore {

    protected final KVStore kvstore;

    /**
     * Constructor.
     *
     * @param kvstore the underlying {@link KVStore}
     * @throws IllegalArgumentException if {@code kvstore} is null
     */
    public ForwardingKVStore(KVStore kvstore) {
        if (kvstore == null)
            throw new IllegalArgumentException("null kvstore");
        this.kvstore = kvstore;
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        return this.kvstore.get(key);
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        return this.kvstore.getAtLeast(minKey);
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        return this.kvstore.getAtMost(maxKey);
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        return this.kvstore.getRange(minKey, maxKey, reverse);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        this.kvstore.put(key, value);
    }

    @Override
    public void remove(byte[] key) {
        this.kvstore.remove(key);
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        this.kvstore.removeRange(minKey, maxKey);
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        this.kvstore.adjustCounter(key, amount);
    }

    @Override
    public byte[] encodeCounter(long value) {
        return this.kvstore.encodeCounter(value);
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        return this.kvstore.decodeCounter(bytes);
    }
}

