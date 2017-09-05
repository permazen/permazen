
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import java.io.File;

import io.permazen.kv.test.AtomicKVStoreTest;

public class AtomicArrayKVStoreTest extends AtomicKVStoreTest {

    @Override
    protected AtomicArrayKVStore createAtomicKVStore(File dir) throws Exception {
        final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
        kvstore.setDirectory(dir);
        return kvstore;
    }
}
