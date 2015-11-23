
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import java.io.File;
import java.io.IOException;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVDatabaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class ArrayKVDatabaseTest extends KVDatabaseTest {

    private ArrayKVDatabase arrayKV;

    @BeforeClass(groups = "configure")
    @Parameters({
      "arrayDirPrefix",
      "arrayCompactMaxDelay",
      "arrayCompactSpaceLowWater",
      "arrayCompactSpaceHighWater",
    })
    public void setArrayDirPrefix(@Optional String arrayDirPrefix,
      @Optional("90") int compactMaxDelay,
      @Optional("65536") int compactLowWater,
      @Optional("1073741824") int compactHighWater) throws IOException {
        if (arrayDirPrefix != null) {
            final File dir = File.createTempFile(arrayDirPrefix, null);
            Assert.assertTrue(dir.delete());
            Assert.assertTrue(dir.mkdirs());
            dir.deleteOnExit();
            final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
            kvstore.setDirectory(dir);
            kvstore.setCompactMaxDelay(compactMaxDelay);
            kvstore.setCompactLowWater(compactLowWater);
            kvstore.setCompactHighWater(compactHighWater);
            this.arrayKV = new ArrayKVDatabase();
            this.arrayKV.setKVStore(kvstore);
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.arrayKV;
    }
}

