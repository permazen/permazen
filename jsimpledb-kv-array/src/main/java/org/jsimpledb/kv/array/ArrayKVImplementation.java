
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import java.io.File;
import java.util.ArrayDeque;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVImplementation;
import org.jsimpledb.kv.mvcc.AtomicKVStore;

public class ArrayKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--arraydb dir", "Use array key/value database in the specified directory" }
        };
    }

    @Override
    public File parseCommandLineOptions(ArrayDeque<String> options) {
        final String arg = this.parseCommandLineOption(options, "--arraydb");
        return arg != null ? new File(arg) : null;
    }

    @Override
    public ArrayKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        final ArrayKVDatabase arraydb = new ArrayKVDatabase();
        arraydb.setKVStore(this.createAtomicKVStore(configuration));
        return arraydb;
    }

    @Override
    public AtomicArrayKVStore createAtomicKVStore(Object configuration) {
        final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
        kvstore.setDirectory((File)configuration);
        return kvstore;
    }

    @Override
    public String getDescription(Object configuration) {
        return "ArrayDB " + ((File)configuration).getName();
    }
}
