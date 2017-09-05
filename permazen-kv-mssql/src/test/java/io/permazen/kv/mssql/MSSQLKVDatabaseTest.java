
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mssql;

import com.jolbox.bonecp.BoneCPDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.sql.IsolationLevel;
import io.permazen.kv.test.KVDatabaseTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class MSSQLKVDatabaseTest extends KVDatabaseTest {

    private MSSQLKVDatabase mssqlKV;
    private BoneCPDataSource dataSource;

    @BeforeClass(groups = "configure")
    @Parameters("mssqlURL")
    public void setMSSQLURL(@Optional String mssqlURL) {
        if (mssqlURL != null) {
            this.dataSource = new BoneCPDataSource();
            this.dataSource.setDriverClass(SQLServerDriver.class.getName());
            this.dataSource.setJdbcUrl(mssqlURL);
            this.dataSource.setStatementsCacheSize(100);
            this.mssqlKV = new MSSQLKVDatabase();
            this.mssqlKV.setDataSource(this.dataSource);
            this.mssqlKV.setIsolationLevel(IsolationLevel.SERIALIZABLE);
            this.mssqlKV.setLockTimeout(100);
        }
    }

    @AfterClass
    public void cleanup() {
        if (this.dataSource != null)
            this.dataSource.close();
    }

    @Override
    protected boolean allowBothTransactionsToFail() {
        return true;
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.mssqlKV;
    }

    @Override
    protected int getParallelTransactionLoopCount() {
        return 13;
    }

    @Override
    protected int getParallelTransactionTaskCount() {
        return 13;
    }

    @Override
    protected int getSequentialTransactionLoopCount() {
        return 13;
    }

    @Override
    protected int getRandomTaskMaxIterations() {
        return 50;
    }
}

