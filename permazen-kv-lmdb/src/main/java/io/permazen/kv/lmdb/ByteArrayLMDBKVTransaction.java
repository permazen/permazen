
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.lmdb;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

/**
 * {@link LMDBKVTransaction} using {@code byte[]} arrays buffers.
 */
public class ByteArrayLMDBKVTransaction extends LMDBKVTransaction<byte[]> {

    /**
     * Constructor.
     */
    protected ByteArrayLMDBKVTransaction(LMDBKVDatabase<byte[]> kvdb, Env<byte[]> env, Dbi<byte[]> db) {
        super(kvdb, env, db);
    }

    @Override
    protected ByteArrayLMDBKVStore createKVStore(Dbi<byte[]> db, Txn<byte[]> tx) {
        return new ByteArrayLMDBKVStore(db, tx);
    }
}
