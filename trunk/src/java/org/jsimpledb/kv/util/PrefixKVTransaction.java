
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import org.jsimpledb.kv.KVTransaction;

/**
 * {@link KVTransaction} view of all keys having a common {@code byte[]} prefix in a containing {@link KVTransaction}.
 *
 * <p>
 * Instances are normally created indirectly from {@link PrefixKVDatabase} instances via {@link PrefixKVDatabase#createTransaction}.
 * Instances may also be created directly from an existing {@link KVTransaction}; in that case,
 * {@link #setTimeout setTimeout()}, {@link #commit}, and {@link #rollback} forward to the containing transaction,
 * while {@link #getKVDatabase} throws {@link UnsupportedOperationException}.
 * </p>
 */
public class PrefixKVTransaction extends PrefixKVStore implements KVTransaction {

    private final PrefixKVDatabase db;

    /**
     * Constructor that wraps an existing {@link KVTransaction}. There will be no associated {@link PrefixKVDatabase}.
     *
     * @param tx the containing {@link KVTransaction}
     * @param keyPrefix prefix for all keys
     * @throws IllegalArgumentException if {@code tx} or {@code keyPrefix} is null
     */
    public PrefixKVTransaction(KVTransaction tx, byte[] keyPrefix) {
        super(tx, keyPrefix);
        this.db = null;
    }

    /**
     * Constructor for when there is an associated {@link PrefixKVDatabase}..
     *
     * @param db the containing {@link PrefixKVDatabase}
     * @throws IllegalArgumentException if {@code db} is null
     */
    PrefixKVTransaction(PrefixKVDatabase db) {
        super(db.getContainingKVDatabase().createTransaction(), db.getKeyPrefix());
        this.db = db;
    }

    /**
     * Get the containing {@link KVTransaction} associated with this instance.
     *
     * @return the containing {@link KVTransaction}
     */
    public KVTransaction getContainingKVTransaction() {
        return (KVTransaction)this.getContainingKVStore();
    }

// KVTransaction

    /**
     * Get the {@link PrefixKVDatabase} associated with this instance.
     *
     * @throws UnsupportedOperationException if this instance was created directly from a containing {@link KVTransaction}
     */
    @Override
    public PrefixKVDatabase getKVDatabase() {
        if (this.db == null)
            throw new UnsupportedOperationException("instance was not created from a PrefixKVDatabase");
        return this.db;
    }

    @Override
    public void setTimeout(long timeout) {
        this.getContainingKVTransaction().setTimeout(timeout);
    }

    @Override
    public void commit() {
        this.getContainingKVTransaction().commit();
    }

    @Override
    public void rollback() {
        this.getContainingKVTransaction().rollback();
    }
}

