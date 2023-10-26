
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.cockroach;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.test.KVDatabaseTest;

import org.postgresql.ds.PGSimpleDataSource;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class CockroachKVDatabaseTest extends KVDatabaseTest {

    private CockroachKVDatabase cockroachKV;

    @BeforeClass(groups = "configure")
    @Parameters("cockroachURL")
    public void setCockroachURL(@Optional String cockroachURL) {
        if (cockroachURL != null) {
            final PGSimpleDataSource dataSource = new PGSimpleDataSource();
            dataSource.setUrl(cockroachURL);
            this.cockroachKV = new CockroachKVDatabase();
            this.cockroachKV.setDataSource(dataSource);
        }
    }

    @Override
    protected boolean allowBothTransactionsToFail() {
        return true;
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.cockroachKV;
    }
}
