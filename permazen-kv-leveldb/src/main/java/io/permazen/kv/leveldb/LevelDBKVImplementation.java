
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;

public class LevelDBKVImplementation extends KVImplementation<File> {

    public LevelDBKVImplementation() {
        super(File.class);
    }

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
    public LevelDBKVDatabase createKVDatabase(File directory, KVDatabase kvdb, AtomicKVStore kvstore) {
        final LevelDBKVDatabase leveldb = new LevelDBKVDatabase();
        leveldb.setKVStore(this.createAtomicKVStore(directory));
        return leveldb;
    }

    @Override
    public LevelDBAtomicKVStore createAtomicKVStore(File directory) {
        final LevelDBAtomicKVStore kvstore = new LevelDBAtomicKVStore();
        kvstore.setDirectory(directory);
        kvstore.setCreateIfMissing(true);
        return kvstore;
    }

    @Override
    public String getDescription(File directory) {
        return "LevelDB " + directory.getName();
    }
}
