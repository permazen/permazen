
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.util.ByteData;

import java.util.concurrent.Future;

/**
 * {@link KVTransaction} view of all keys having a common {@code byte[]} prefix in a containing {@link KVTransaction}.
 *
 * <p>
 * Instances are normally created indirectly from {@link PrefixKVDatabase} instances via {@link PrefixKVDatabase#createTransaction}.
 * Instances may also be created directly from an existing {@link KVTransaction}; in that case,
 * {@link #setTimeout setTimeout()}, {@link #commit}, and {@link #rollback} forward to the containing transaction,
 * while {@link #getKVDatabase} throws {@link UnsupportedOperationException}.
 */
public class PrefixKVTransaction extends PrefixKVStore implements KVTransaction {

    private final KVTransaction tx;
    private final PrefixKVDatabase db;

// Constructors

    /**
     * Constructor that wraps an existing {@link KVTransaction}. There will be no associated {@link PrefixKVDatabase}.
     *
     * @param tx the containing {@link KVTransaction}
     * @param keyPrefix prefix for all keys
     * @throws IllegalArgumentException if {@code tx} or {@code keyPrefix} is null
     */
    public PrefixKVTransaction(KVTransaction tx, ByteData keyPrefix) {
        this(tx, keyPrefix, null);
    }

    /**
     * Constructor for when there is an associated {@link PrefixKVDatabase}.
     *
     * @param db the containing {@link PrefixKVDatabase}
     * @throws NullPointerException if {@code db} is null
     */
    PrefixKVTransaction(PrefixKVDatabase db) {
        this(db.getContainingKVDatabase().createTransaction(), db.getKeyPrefix(), db);
    }

    private PrefixKVTransaction(KVTransaction tx, ByteData keyPrefix, PrefixKVDatabase db) {
        super(keyPrefix);
        Preconditions.checkArgument(tx != null, "null tx");
        this.tx = tx;
        this.db = db;
    }

// PrefixKVStore

    @Override
    protected KVTransaction delegate() {
        return this.tx;
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
        this.delegate().setTimeout(timeout);
    }

    @Override
    public Future<Void> watchKey(ByteData key) {
        Preconditions.checkArgument(key != null, "null key");
        return this.delegate().watchKey(this.db.getKeyPrefix().concat(key));
    }

    @Override
    public boolean isReadOnly() {
        return this.delegate().isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.delegate().setReadOnly(readOnly);
    }

    @Override
    public void commit() {
        this.delegate().commit();
    }

    @Override
    public void rollback() {
        this.delegate().rollback();
    }

    @Override
    public CloseableKVStore readOnlySnapshot() {
        final CloseableKVStore kvstore = this.tx.readOnlySnapshot();
        return new CloseableForwardingKVStore(PrefixKVStore.create(kvstore, this.getKeyPrefix()), kvstore::close);
    }
}
