
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.test.KVDatabaseTest;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class MVStoreKVDatabaseTest extends KVDatabaseTest {

    private MVStoreKVDatabase kvdb;

    @BeforeClass(groups = "configure")
    @Parameters({ "mvstoreFilePrefix", "mvstoreCompress", "mvstoreCompressHigh", "mvstoreEncryptKey" })
    public void configureMVStore(
      @Optional String filePrefix,
      @Optional Boolean compress,
      @Optional Boolean compressHigh,
      @Optional String encryptKey) throws IOException {

        // Enabled?
        if (filePrefix == null)
            return;

        // Create config
        final MVStoreKVImplementation.Config config = new MVStoreKVImplementation.Config();

        // Configure temporary file (or in-memory)
        if (!filePrefix.equals("MEMORY")) {
            final File file = File.createTempFile(filePrefix, ".mvstore");
            file.delete();
            file.deleteOnExit();
            config.setFile(file);
        } else
            config.setMemory(true);

        // Configure KVStore
        if (compress != null)
            config.setCompress(compress);
        if (compressHigh != null)
            config.setCompressHigh(compressHigh);
        if (encryptKey != null && !encryptKey.isEmpty())
            config.setEncryptKey(encryptKey);
        final MVStoreAtomicKVStore kvstore = config.configure(new MVStoreAtomicKVStore());

        // Configure DB
        this.kvdb = new MVStoreKVDatabase();
        this.kvdb.setKVStore(kvstore);
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.kvdb;
    }
}
