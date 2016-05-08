
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.fdb;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.test.KVDatabaseTest;
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
}

