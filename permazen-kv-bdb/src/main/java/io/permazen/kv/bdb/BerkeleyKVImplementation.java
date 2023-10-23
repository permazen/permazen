
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.bdb;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;

public class BerkeleyKVImplementation extends KVImplementation<BerkeleyKVImplementation.Config> {

    public BerkeleyKVImplementation() {
        super(Config.class);
    }

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--bdb directory",
              "Use Berkeley DB Java Edition key/value database in specified directory" },
            { "--bdb-database name",
              "Specify Berkeley DB database name (default \"" + BerkeleyKVDatabase.DEFAULT_DATABASE_NAME + "\")" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        final String dir = this.parseCommandLineOption(options, "--bdb");
        if (dir == null)
            return null;
        final Config config = new Config(new File(dir));
        final String dbname = this.parseCommandLineOption(options, "--bdb-database");
        if (dbname != null)
            config.setDatabaseName(dbname);
        return config;
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
        private String databaseName = BerkeleyKVDatabase.DEFAULT_DATABASE_NAME;

        public Config(File dir) {
            if (dir == null)
                throw new IllegalArgumentException("null dir");
            this.dir = dir;
        }

        public File getDirectory() {
            return this.dir;
        }

        public String getDatabaseName() {
            return this.databaseName;
        }

        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }
    }
}
