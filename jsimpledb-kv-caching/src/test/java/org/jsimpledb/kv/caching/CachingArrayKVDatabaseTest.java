
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.caching;

import java.io.File;
import java.io.IOException;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.array.ArrayKVDatabase;
import org.jsimpledb.kv.array.AtomicArrayKVStore;
import org.jsimpledb.kv.test.KVDatabaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class CachingArrayKVDatabaseTest extends KVDatabaseTest {

    private CachingKVDatabase kvdb;

    @BeforeClass(groups = "configure")
    @Parameters({
      "testCachingKV",
      "arrayDirPrefix",
      "arrayCompactMaxDelay",
      "arrayCompactSpaceLowWater",
      "arrayCompactSpaceHighWater",
    })
    public void setTestCachingKV(
      @Optional String testCachingKV,
      @Optional String arrayDirPrefix,
      @Optional("90") int compactMaxDelay,
      @Optional("65536") int compactLowWater,
      @Optional("1073741824") int compactHighWater) throws IOException {
        if (testCachingKV != null && Boolean.valueOf(testCachingKV) && arrayDirPrefix != null) {
            final File dir = File.createTempFile(arrayDirPrefix, null);
            Assert.assertTrue(dir.delete());
            Assert.assertTrue(dir.mkdirs());
            dir.deleteOnExit();
            final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
            kvstore.setDirectory(dir);
            kvstore.setCompactMaxDelay(compactMaxDelay);
            kvstore.setCompactLowWater(compactLowWater);
            kvstore.setCompactHighWater(compactHighWater);
            final ArrayKVDatabase arrayKV = new ArrayKVDatabase();
            arrayKV.setKVStore(kvstore);
            this.kvdb = new CachingKVDatabase(arrayKV);
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.kvdb;
    }
}
