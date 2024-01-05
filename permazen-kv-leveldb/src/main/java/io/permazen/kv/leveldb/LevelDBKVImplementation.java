
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class LevelDBKVImplementation implements KVImplementation<File> {

    private OptionSpec<File> directoryOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.directoryOption == null, "duplicate option");
        this.directoryOption = parser.accepts("leveldb", "Use LevelDB key/value database (or key/value store)")
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
    public boolean providesKVDatabase(File config) {
        return true;
    }

    @Override
    public boolean providesAtomicKVStore(File config) {
        return true;
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
