
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.bdb;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.test.KVDatabaseTest;

import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class BerkeleyKVDatabaseTest extends KVDatabaseTest {

    private BerkeleyKVDatabase bdbKV;

    @BeforeClass(groups = "configure")
    @Parameters("berkeleyDirPrefix")
    public void setBerkeleyDirPrefix(@Optional String berkeleyDirPrefix) throws IOException {
        if (berkeleyDirPrefix != null) {
            final File dir = File.createTempFile(berkeleyDirPrefix, null);
            Assert.assertTrue(dir.delete());
            Assert.assertTrue(dir.mkdirs());
            dir.deleteOnExit();
            this.bdbKV = new BerkeleyKVDatabase();
            this.bdbKV.setDirectory(dir);
        }
    }

    @Override
    protected boolean allowBothTransactionsToFail() {
        return true;
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.bdbKV;
    }
}
