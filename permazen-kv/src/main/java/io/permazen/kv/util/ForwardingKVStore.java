
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

/**
 * Forwards all {@link KVStore} operations to another underlying {@link KVStore}.
 */
public abstract class ForwardingKVStore implements KVStore {

    /**
     * Get the underlying {@link KVStore}.
     *
     * @return underlying {@link KVStore}
     */
    protected abstract KVStore delegate();

// KVStore

    @Override
    public ByteData get(ByteData key) {
        return this.delegate().get(key);
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        return this.delegate().getAtLeast(minKey, maxKey);
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        return this.delegate().getAtMost(maxKey, minKey);
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        return this.delegate().getRange(minKey, maxKey, reverse);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        this.delegate().put(key, value);
    }

    @Override
    public void remove(ByteData key) {
        this.delegate().remove(key);
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        this.delegate().removeRange(minKey, maxKey);
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        this.delegate().adjustCounter(key, amount);
    }

    @Override
    public ByteData encodeCounter(long value) {
        return this.delegate().encodeCounter(value);
    }

    @Override
    public long decodeCounter(ByteData bytes) {
        return this.delegate().decodeCounter(bytes);
    }

    @Override
    public void apply(Mutations mutations) {
        this.delegate().apply(mutations);
    }
}
