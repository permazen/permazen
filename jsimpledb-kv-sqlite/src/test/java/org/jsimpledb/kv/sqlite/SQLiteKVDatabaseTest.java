
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sqlite;

import java.io.File;
import java.io.IOException;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.test.KVDatabaseTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class SQLiteKVDatabaseTest extends KVDatabaseTest {

    private SQLiteKVDatabase kvdb;

    @BeforeClass(groups = "configure")
    @Parameters("sqliteFilePrefix")
    public void setFilePrefix(@Optional String filePrefix) throws IOException {
        if (filePrefix != null) {
            final File file = File.createTempFile(filePrefix, ".sqlite3");
            this.kvdb = new SQLiteKVDatabase();
            this.kvdb.setDatabaseFile(file);
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.kvdb;
    }
}

