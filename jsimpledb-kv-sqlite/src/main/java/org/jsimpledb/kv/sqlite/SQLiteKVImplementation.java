
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sqlite;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVImplementation;
import org.jsimpledb.kv.mvcc.AtomicKVStore;

public class SQLiteKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--sqlite file",          "Use SQLite key/value database using the specified file" },
            { "--sqlite-exclusive",     "Configure SQLite connections for exclusive locking" },
            { "--sqlite-pragma pragma", "Specify a PRAGMA for new connections (omit PRAGMA keyword); may be repeated" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        final String filename = this.parseCommandLineOption(options, "--sqlite");
        if (filename == null)
            return null;
        final Config config = new Config();
        config.setFile(new File(filename));
        config.setExclusiveLocking(this.parseCommandLineFlag(options, "--sqlite-exclusive"));
        String pragma;
        while ((pragma = this.parseCommandLineOption(options, "--sqlite-pragma")) != null)
            config.getPragmas().add(pragma);
        return config;
    }

    @Override
    public SQLiteKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        final Config config = (Config)configuration;
        final SQLiteKVDatabase sqlite = new SQLiteKVDatabase();
        sqlite.setDatabaseFile(config.getFile());
        sqlite.setExclusiveLocking(config.isExclusiveLocking());
        sqlite.setPragmas(config.getPragmas());
        return sqlite;
    }

    @Override
    public String getDescription(Object configuration) {
        final Config config = (Config)configuration;
        return "SQLite " + config.getFile().getName();
    }

// Config

    private static class Config {

        private File file;
        private boolean exclusiveLocking;
        private final ArrayList<String> pragmas = new ArrayList<>();

        public File getFile() {
            return this.file;
        }
        public void setFile(File file) {
            this.file = file;
        }

        public boolean isExclusiveLocking() {
            return this.exclusiveLocking;
        }
        public void setExclusiveLocking(boolean exclusiveLocking) {
            this.exclusiveLocking = exclusiveLocking;
        }

        public ArrayList<String> getPragmas() {
            return this.pragmas;
        }
    }
}
