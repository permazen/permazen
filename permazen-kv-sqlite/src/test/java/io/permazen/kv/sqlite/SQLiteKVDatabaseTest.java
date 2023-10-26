
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.sqlite;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.test.KVDatabaseTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class SQLiteKVDatabaseTest extends KVDatabaseTest {

    private SQLiteKVDatabase kvdb;

    @BeforeClass(groups = "configure")
    @Parameters({ "sqliteFilePrefix", "exclusiveLocking", "pragmas" })
    public void setFilePrefix(
      @Optional String filePrefix,
      @Optional("true") boolean exclusiveLocking,
      @Optional String pragmas) throws IOException {
        if (filePrefix != null) {
            final File file = File.createTempFile(filePrefix, ".sqlite3");
            this.kvdb = new SQLiteKVDatabase();
            this.kvdb.setDatabaseFile(file);
            this.kvdb.setExclusiveLocking(exclusiveLocking);
            if (pragmas != null) {
                final ArrayList<String> pragmaList = new ArrayList<>();
                for (String pragma : pragmas.split("\\s*,\\s*"))
                    pragmaList.add(pragma);
                this.kvdb.setPragmas(pragmaList);
            }
        }
    }

    protected boolean allowBothTransactionsToFail() {
        return true;
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.kvdb;
    }
}
