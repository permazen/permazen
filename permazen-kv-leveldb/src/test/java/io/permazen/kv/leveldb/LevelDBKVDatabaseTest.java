
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import java.io.File;
import java.io.IOException;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.test.KVDatabaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class LevelDBKVDatabaseTest extends KVDatabaseTest {

    private LevelDBKVDatabase leveldbKV;

    @BeforeClass(groups = "configure")
    @Parameters("levelDbDirPrefix")
    public void setLevelDbDirPrefix(@Optional String levelDbDirPrefix) throws IOException {
        if (levelDbDirPrefix != null) {
            final File dir = File.createTempFile(levelDbDirPrefix, null);
            Assert.assertTrue(dir.delete());
            Assert.assertTrue(dir.mkdirs());
            dir.deleteOnExit();
            final LevelDBAtomicKVStore kvstore = new LevelDBAtomicKVStore();
            kvstore.setDirectory(dir);
            kvstore.setCreateIfMissing(true);
            this.leveldbKV = new LevelDBKVDatabase();
            this.leveldbKV.setKVStore(kvstore);
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.leveldbKV;
    }
}

