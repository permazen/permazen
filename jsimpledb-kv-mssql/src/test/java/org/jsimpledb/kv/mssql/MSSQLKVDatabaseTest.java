
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mssql;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.sql.IsolationLevel;
import org.jsimpledb.kv.test.KVDatabaseTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class MSSQLKVDatabaseTest extends KVDatabaseTest {

    private MSSQLKVDatabase mssqlKV;

    @BeforeClass(groups = "configure")
    @Parameters("mssqlURL")
    public void setMSSQLURL(@Optional String mssqlURL) {
        if (mssqlURL != null) {
            final SQLServerDataSource dataSource = new SQLServerDataSource();
            dataSource.setURL(mssqlURL);
            this.mssqlKV = new MSSQLKVDatabase();
            this.mssqlKV.setDataSource(dataSource);
            this.mssqlKV.setLockTimeout(100);
        }
    }

    @Override
    protected boolean allowBothTransactionsToFail() {
        return true;
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.mssqlKV;
    }
}

