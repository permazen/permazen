
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import java.io.File;

import org.jsimpledb.kv.test.AtomicKVStoreTest;

public class AtomicArrayKVStoreTest extends AtomicKVStoreTest {

    @Override
    protected AtomicArrayKVStore createAtomicKVStore(File dir) throws Exception {
        final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
        kvstore.setDirectory(dir);
        return kvstore;
    }
}
