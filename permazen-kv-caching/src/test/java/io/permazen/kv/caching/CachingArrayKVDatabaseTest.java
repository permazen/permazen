
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.caching;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.array.ArrayKVDatabase;
import io.permazen.kv.array.AtomicArrayKVStore;
import io.permazen.kv.test.KVDatabaseTest;

import java.io.File;
import java.io.IOException;

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
