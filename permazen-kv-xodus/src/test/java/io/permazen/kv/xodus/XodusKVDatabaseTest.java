
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.xodus;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.test.KVDatabaseTest;

import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class XodusKVDatabaseTest extends KVDatabaseTest {

    private XodusKVDatabase xodusKV;

    @BeforeClass(groups = "configure")
    @Parameters("xodusDirPrefix")
    public void setXodusDirPrefix(@Optional String xodusDirPrefix) throws IOException {
        if (xodusDirPrefix != null) {
            final File dir = File.createTempFile(xodusDirPrefix, null);
            Assert.assertTrue(dir.delete());
            Assert.assertTrue(dir.mkdirs());
            dir.deleteOnExit();
            this.xodusKV = new XodusKVDatabase();
            this.xodusKV.setDirectory(dir);
        }
    }

    @Override
    protected boolean supportsReadOnlyAfterDataAccess() {
        return false;
    }

    @Override
    protected boolean transactionsAreThreadSafe() {
        return false;
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.xodusKV;
    }
}
