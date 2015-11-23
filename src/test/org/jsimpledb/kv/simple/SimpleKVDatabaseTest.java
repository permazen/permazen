
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.simple;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVDatabaseTest;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class SimpleKVDatabaseTest extends KVDatabaseTest {

    private SimpleKVDatabase simpleKV;

    @BeforeClass(groups = "configure")
    @Parameters("testSimpleKV")
    public void setTestSimpleKV(@Optional String testSimpleKV) {
        if (testSimpleKV != null && Boolean.valueOf(testSimpleKV))
            this.simpleKV = new SimpleKVDatabase(new NavigableMapKVStore(), 250, 5000);
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.simpleKV;
    }
}

