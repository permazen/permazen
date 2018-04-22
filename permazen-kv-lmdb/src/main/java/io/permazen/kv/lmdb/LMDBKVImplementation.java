
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.lmdb;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;

public class LMDBKVImplementation extends KVImplementation<LMDBKVImplementation.Config> {

    public LMDBKVImplementation() {
        super(Config.class);
    }

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--lmdb directory",  "Use LMDB key/value database in the specified directory" },
            { "--lmdb-dbname name", "Specify LMDB database name (default \"" + LMDBKVDatabase.DEFAULT_DATABASE_NAME + "\")" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        String arg = this.parseCommandLineOption(options, "--lmdb");
        if (arg == null)
            return null;
        final Config config = new Config();
        config.setDirectory(new File(arg));
        if ((arg = this.parseCommandLineOption(options, "--lmdb-dbname")) != null)
            config.setDatabaseName(arg);
        return config;
    }

    @Override
    public LMDBKVDatabase<?> createKVDatabase(Config config, KVDatabase ignored, AtomicKVStore kvstore) {
        final ByteArrayLMDBKVDatabase kvdb = new ByteArrayLMDBKVDatabase();
        config.configure(kvdb);
        return kvdb;
    }

    @Override
    public String getDescription(Config config) {
        return "LMDB " + config.getDirectory().getName();
    }

// Options

    public static class Config {

        private File directory;
        private String databaseName;

        public File getDirectory() {
            return this.directory;
        }
        public void setDirectory(File directory) {
            this.directory = directory;
        }

        public String getDatabaseName() {
            return this.databaseName;
        }
        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }

        public void configure(LMDBKVDatabase<?> kvdb) {
            Preconditions.checkArgument(this.directory != null, "LMDB directory must be specified via the `--lmdb' flag");
            kvdb.setDirectory(this.directory);
            if (this.databaseName != null)
                kvdb.setDatabaseName(this.databaseName);
        }
    }
}
