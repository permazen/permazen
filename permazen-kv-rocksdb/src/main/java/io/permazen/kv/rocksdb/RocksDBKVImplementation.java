
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.rocksdb;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;

public class RocksDBKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--rocksdb directory", "Use RocksDB key/value database in the specified directory" },
        };
    }

    @Override
    public File parseCommandLineOptions(ArrayDeque<String> options) {
        final String arg = this.parseCommandLineOption(options, "--rocksdb");
        return arg != null ? new File(arg) : null;
    }

    @Override
    public RocksDBKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        final RocksDBKVDatabase rocksdb = new RocksDBKVDatabase();
        rocksdb.setKVStore(this.createAtomicKVStore(configuration));
        return rocksdb;
    }

    @Override
    public RocksDBAtomicKVStore createAtomicKVStore(Object configuration) {
        final RocksDBAtomicKVStore kvstore = new RocksDBAtomicKVStore();
        kvstore.setDirectory((File)configuration);
        return kvstore;
    }

    @Override
    public String getDescription(Object configuration) {
        return "RocksDB " + ((File)configuration).getName();
    }
}
