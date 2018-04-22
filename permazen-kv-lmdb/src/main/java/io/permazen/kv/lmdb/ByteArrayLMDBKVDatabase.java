
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.lmdb;

import java.util.Map;

import org.lmdbjava.ByteArrayProxy;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;

/**
 * {@link LMDBKVDatabase} using {@code byte[]} arrays buffers.
 */
public class ByteArrayLMDBKVDatabase extends LMDBKVDatabase<byte[]> {

    public ByteArrayLMDBKVDatabase() {
        super(Env.create(ByteArrayProxy.PROXY_BA));
    }

    @Override
    protected ByteArrayLMDBKVTransaction doCreateTransaction(Env<byte[]> env, Dbi<byte[]> db, Map<String, ?> options) {
        return new ByteArrayLMDBKVTransaction(this, env, db);
    }
}
