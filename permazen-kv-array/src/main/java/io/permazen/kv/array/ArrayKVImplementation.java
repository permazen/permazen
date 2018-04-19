
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;

public class ArrayKVImplementation extends KVImplementation<File> {

    public ArrayKVImplementation() {
        super(File.class);
    }

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
    public ArrayKVDatabase createKVDatabase(File directory, KVDatabase kvdb, AtomicKVStore kvstore) {
        final ArrayKVDatabase arraydb = new ArrayKVDatabase();
        arraydb.setKVStore(this.createAtomicKVStore(directory));
        return arraydb;
    }

    @Override
    public AtomicArrayKVStore createAtomicKVStore(File directory) {
        final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
        kvstore.setDirectory(directory);
        return kvstore;
    }

    @Override
    public String getDescription(File directory) {
        return "ArrayDB " + directory.getName();
    }
}
