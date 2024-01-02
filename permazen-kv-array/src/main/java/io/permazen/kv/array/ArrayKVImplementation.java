
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class ArrayKVImplementation implements KVImplementation<File> {

    private OptionSpec<File> directoryOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.directoryOption == null, "duplicate option");
        this.directoryOption = parser.accepts("arraydb", "Use array key/value database in the specified directory")
          .withRequiredArg()
          .describedAs("directory")
          .ofType(File.class);
    }

    @Override
    public File buildConfig(OptionSet options) {
        final File dir = this.directoryOption.value(options);
        if (dir == null)
            return null;
        if (dir.exists() && !dir.isDirectory())
            throw new IllegalArgumentException(String.format("file \"%s\" is not a directory", dir));
        return dir;
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
