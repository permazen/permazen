
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sql;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.test.KVDatabaseTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class MySQLKVDatabaseTest extends KVDatabaseTest {

    private MySQLKVDatabase mysqlKV;

    @BeforeClass(groups = "configure")
    @Parameters("mysqlURL")
    public void setMySQLURL(@Optional String mysqlURL) {
        if (mysqlURL != null) {
            final MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setUrl(mysqlURL);
            this.mysqlKV = new MySQLKVDatabase();
            this.mysqlKV.setDataSource(dataSource);
            this.mysqlKV.setIsolationLevel(IsolationLevel.SERIALIZABLE);
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.mysqlKV;
    }
}

