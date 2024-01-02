
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.bdb;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class BerkeleyKVImplementation implements KVImplementation<BerkeleyKVImplementation.Config> {

    private OptionSpec<File> directoryOption;
    private OptionSpec<String> databaseNameOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.directoryOption == null, "duplicate option");
        Preconditions.checkState(this.databaseNameOption == null, "duplicate option");
        this.directoryOption = parser.accepts("bdb", "Use Berkeley DB Java Edition key/value database in specified directory")
          .withRequiredArg()
          .describedAs("directory")
          .ofType(File.class);
        this.databaseNameOption = parser.accepts("bdb-database",
            String.format("Specify Berkeley DB database name (default \"%s\")", BerkeleyKVDatabase.DEFAULT_DATABASE_NAME))
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("database-name")
          .defaultsTo(BerkeleyKVDatabase.DEFAULT_DATABASE_NAME);
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
    public BerkeleyKVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final BerkeleyKVDatabase bdb = new BerkeleyKVDatabase();
        bdb.setDirectory(config.getDirectory());
        bdb.setDatabaseName(config.getDatabaseName());
        return bdb;
    }

    @Override
    public String getDescription(Config config) {
        return "BerkeleyDB " + config.getDirectory().getName();
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
