
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sqlite;

import java.io.File;
import java.util.ArrayDeque;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVImplementation;
import org.jsimpledb.kv.mvcc.AtomicKVStore;

public class SQLiteKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--sqlite file", "Use SQLite key/value database using the specified file" },
        };
    }

    @Override
    public File parseCommandLineOptions(ArrayDeque<String> options) {
        final String filename = this.parseCommandLineOption(options, "--sqlite");
        return filename != null ? new File(filename) : null;
    }

    @Override
    public SQLiteKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        final SQLiteKVDatabase sqlite = new SQLiteKVDatabase();
        sqlite.setDatabaseFile((File)configuration);
        return sqlite;
    }

    @Override
    public String getDescription(Object configuration) {
        return "SQLite " + ((File)configuration).getName();
    }
}
