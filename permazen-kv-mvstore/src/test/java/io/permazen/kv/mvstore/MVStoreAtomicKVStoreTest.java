
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import io.permazen.kv.test.AtomicKVStoreTest;

import java.io.File;

public class MVStoreAtomicKVStoreTest extends AtomicKVStoreTest {

    @Override
    protected MVStoreAtomicKVStore createAtomicKVStore(File dir) throws Exception {

        // Create config
        final MVStoreKVImplementation.Config config = new MVStoreKVImplementation.Config();

        // Configure temporary file
        final File file = File.createTempFile("MVStoreAtomicKVStoreTest", ".mvstore", dir);
        file.delete();
        file.deleteOnExit();
        config.setFile(file);

        // Build store
        return config.configure(new MVStoreAtomicKVStore());
    }
}
