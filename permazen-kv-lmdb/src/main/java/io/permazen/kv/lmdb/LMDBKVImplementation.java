
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.lmdb;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class LMDBKVImplementation implements KVImplementation<LMDBKVImplementation.Config> {

    private OptionSpec<File> directoryOption;
    private OptionSpec<String> databaseNameOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.directoryOption == null, "duplicate option");
        Preconditions.checkState(this.databaseNameOption == null, "duplicate option");
        this.directoryOption = parser.accepts("lmdb", "Use LMDB key/value database in the specified directory")
          .withRequiredArg()
          .describedAs("directory")
          .ofType(File.class);
        this.databaseNameOption = parser.accepts("lmdb-dbname",
            String.format("Specify LMDB database name (default \"%s\")", LMDBKVDatabase.DEFAULT_DATABASE_NAME))
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("database-name")
          .defaultsTo(LMDBKVDatabase.DEFAULT_DATABASE_NAME);
    }

    @Override
    public Config buildConfig(OptionSet options) {
        final File dir = this.directoryOption.value(options);
        if (dir == null)
            return null;
        if (dir.exists() && !dir.isDirectory())
            throw new IllegalArgumentException(String.format("file \"%s\" is not a directory", dir));
        return new Config(dir, this.databaseNameOption.value(options));
    }

    @Override
    public LMDBKVDatabase<?> createKVDatabase(Config config, KVDatabase ignored, AtomicKVStore kvstore) {
        final ByteArrayLMDBKVDatabase kvdb = new ByteArrayLMDBKVDatabase();
        kvdb.setDirectory(config.getDirectory());
        kvdb.setDatabaseName(config.getDatabaseName());
        return kvdb;
    }

    @Override
    public String getDescription(Config config) {
        return "LMDB " + config.getDirectory().getName();
    }

// Config

    public static class Config {

        private File dir;
        private String databaseName;

        public Config(File dir, String databaseName) {
            Preconditions.checkArgument(dir != null, "null dir");
            Preconditions.checkArgument(databaseName != null, "null databaseName");
            this.dir = dir;
            this.databaseName = databaseName;
        }

        public File getDirectory() {
            return this.dir;
        }

        public String getDatabaseName() {
            return this.databaseName;
        }
    }
}
