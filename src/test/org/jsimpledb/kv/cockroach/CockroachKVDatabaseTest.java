
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.cockroach;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVDatabaseTest;
import org.postgresql.ds.PGPoolingDataSource;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class CockroachKVDatabaseTest extends KVDatabaseTest {

    private CockroachKVDatabase cockroachKV;

    @BeforeClass(groups = "configure")
    @Parameters("cockroachURL")
    public void setCockroachURL(@Optional String cockroachURL) {
        if (cockroachURL != null) {
            final PGPoolingDataSource dataSource = new PGPoolingDataSource();
            dataSource.setUrl(cockroachURL);
            this.cockroachKV = new CockroachKVDatabase();
            this.cockroachKV.setDataSource(dataSource);
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.cockroachKV;
    }
}

