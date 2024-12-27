
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;

/**
 * Provides a read-only view of an underlying {@link KVStore}.
 *
 * <p>
 * Attempts to invoke any of the mutating {@link KVStore} methods result in an {@link UnsupportedOperationException}.
 */
public class UnmodifiableKVStore extends ForwardingKVStore {

    private final KVStore kvstore;

    /**
     * Constructor.
     *
     * @param kvstore the underlying {@link KVStore}
     * @throws IllegalArgumentException if {@code kvstore} is null
     */
    public UnmodifiableKVStore(KVStore kvstore) {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        this.kvstore = kvstore;
    }

    @Override
    protected KVStore delegate() {
        return this.kvstore;
    }

// KVStore

    @Override
    public void put(ByteData key, ByteData value) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void remove(ByteData key) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }
}
