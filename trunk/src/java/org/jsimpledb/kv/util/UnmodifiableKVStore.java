
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import org.jsimpledb.kv.KVStore;

/**
 * Provides a read-only view of an underlying {@link KVStore}.
 *
 * <p>
 * Attempts to mutate the underlying {@link KVStore} result in a {@link UnsupportedOperationException}.
 * </p>
 */
public class UnmodifiableKVStore extends ForwardingKVStore {

    /**
     * Constructor.
     *
     * @param kvstore the underlying {@link KVStore}
     * @throws IllegalArgumentException if {@code kvstore} is null
     */
    public UnmodifiableKVStore(KVStore kvstore) {
        super(kvstore);
    }

    /**
     * Get the underlying {@link KVStore} associated with this instance.
     *
     * @return the underlying {@link KVStore}
     */
    public KVStore getUnderlyingKVStore() {
        return this.kvstore;
    }

// KVStore

    @Override
    public void put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void remove(byte[] key) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        if (amount != 0)
            throw new UnsupportedOperationException("KVStore is read-only");
    }
}

