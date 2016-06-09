
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.rocksdb;

import java.io.File;
import java.util.ArrayDeque;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVImplementation;
import org.jsimpledb.kv.mvcc.AtomicKVStore;

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
