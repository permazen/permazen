
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.rocksdb;

import java.io.File;
import java.io.IOException;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVDatabaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class RocksDBKVDatabaseTest extends KVDatabaseTest {

    private RocksDBKVDatabase rocksdbKV;

    @BeforeClass(groups = "configure")
    @Parameters("rocksDbDirPrefix")
    public void setRocksDBDirPrefix(@Optional String rocksDBDirPrefix) throws IOException {
        if (rocksDBDirPrefix != null) {
            final File dir = File.createTempFile(rocksDBDirPrefix, null);
            Assert.assertTrue(dir.delete());
            Assert.assertTrue(dir.mkdirs());
            dir.deleteOnExit();
            final RocksDBAtomicKVStore kvstore = new RocksDBAtomicKVStore();
            kvstore.setDirectory(dir);
            this.rocksdbKV = new RocksDBKVDatabase();
            this.rocksdbKV.setKVStore(kvstore);
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.rocksdbKV;
    }
}

