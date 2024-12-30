
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.lmdb;

import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;

import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

/**
 * {@link KVStore} view of a LMDB transaction using {@code byte[]} array buffers.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to avoid leaking resources.
 */
public class ByteArrayLMDBKVStore extends LMDBKVStore<byte[]> {

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Closing this instance does <i>not</i> close the underlying transaction.
     *
     * @param db LMDB database
     * @param tx LMDB transaction
     * @throws IllegalArgumentException if {@code db} or {@code tx} is null
     */
    public ByteArrayLMDBKVStore(Dbi<byte[]> db, Txn<byte[]> tx) {
        super(db, tx);
    }

    @Override
    protected byte[] wrap(ByteData data, boolean copy) {
        return data != null ? data.toByteArray() : null;
    }

    @Override
    protected ByteData unwrap(byte[] data, boolean copy) {
        return data != null ? ByteData.of(data) : null;
    }
}
