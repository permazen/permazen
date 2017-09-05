
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;

public class LevelDBKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--leveldb directory", "Use LevelDB key/value database in the specified directory" },
        };
    }

    @Override
    public File parseCommandLineOptions(ArrayDeque<String> options) {
        final String arg = this.parseCommandLineOption(options, "--leveldb");
        return arg != null ? new File(arg) : null;
    }

    @Override
    public LevelDBKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        final LevelDBKVDatabase leveldb = new LevelDBKVDatabase();
        leveldb.setKVStore(this.createAtomicKVStore(configuration));
        return leveldb;
    }

    @Override
    public LevelDBAtomicKVStore createAtomicKVStore(Object configuration) {
        final LevelDBAtomicKVStore kvstore = new LevelDBAtomicKVStore();
        kvstore.setDirectory((File)configuration);
        kvstore.setCreateIfMissing(true);
        return kvstore;
    }

    @Override
    public String getDescription(Object configuration) {
        return "LevelDB " + ((File)configuration).getName();
    }
}
