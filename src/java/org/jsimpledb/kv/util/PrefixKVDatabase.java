
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import org.jsimpledb.kv.KVDatabase;

/**
 * Prefix {@link KVDatabase} implementation.
 *
 * <p>
 * Instances use a fixed {@code byte[]} key prefix for all key access, thereby providing a nested {@link KVDatabase}
 * view of the corresponding sub-range of keys within the containing {@link KVDatabase}. This allows, for example, multiple
 * {@link KVDatabase}s to exist within a single containing {@link KVDatabase} under different key prefixes.
 * </p>
 */
public class PrefixKVDatabase implements KVDatabase {

    private final KVDatabase db;
    private final byte[] keyPrefix;

    /**
     * Constructor.
     *
     * @param db the containing {@link KVDatabase}
     * @param keyPrefix prefix for all keys
     * @throws IllegalArgumentException if {@code db} or {@code keyPrefix} is null
     */
    public PrefixKVDatabase(KVDatabase db, byte[] keyPrefix) {
        if (db == null)
            throw new IllegalStateException("null db");
        if (keyPrefix == null)
            throw new IllegalStateException("null keyPrefix");
        this.db = db;
        this.keyPrefix = keyPrefix.clone();
    }

    /**
     * Get the containing {@link KVDatabase} associated with this instance.
     *
     * @return the containing {@link KVDatabase}
     */
    public KVDatabase getContainingKVDatabase() {
        return this.db;
    }

    /**
     * Get the key prefix associated with this instance.
     *
     * @return (a copy of) this instance's key prefix
     */
    public final byte[] getKeyPrefix() {
        return this.keyPrefix.clone();
    }

    @Override
    public PrefixKVTransaction createTransaction() {
        return new PrefixKVTransaction(this);
    }
}

