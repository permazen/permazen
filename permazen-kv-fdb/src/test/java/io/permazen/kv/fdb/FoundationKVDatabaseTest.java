
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.fdb;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.test.KVDatabaseTest;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class FoundationKVDatabaseTest extends KVDatabaseTest {

    private FoundationKVDatabase fdbKV;

    @BeforeClass(groups = "configure")
    @Parameters("fdbClusterFile")
    public void setFoundationDBClusterFile(@Optional String fdbClusterFile) {
        if (fdbClusterFile != null) {
            this.fdbKV = new FoundationKVDatabase();
            this.fdbKV.setClusterFilePath(fdbClusterFile);
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.fdbKV;
    }

    @Override
    protected boolean supportsReadOnlyAfterDataAccess() {
        return false;
    }
}
