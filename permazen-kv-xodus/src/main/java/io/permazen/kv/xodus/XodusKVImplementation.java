
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.xodus;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;

public class XodusKVImplementation extends KVImplementation<XodusKVImplementation.Config> {

    public XodusKVImplementation() {
        super(Config.class);
    }

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--xodus directory",  "Use Xodus key/value database in the specified directory" },
            { "--xodus-store name", "Specify Xodus store name (default \"" + XodusKVDatabase.DEFAULT_STORE_NAME + "\")" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        String arg = this.parseCommandLineOption(options, "--xodus");
        if (arg == null)
            return null;
        final Config config = new Config();
        config.setDirectory(new File(arg));
        if ((arg = this.parseCommandLineOption(options, "--xodus-store")) != null)
            config.setStoreName(arg);
        return config;
    }

    @Override
    public XodusKVDatabase createKVDatabase(Config config, KVDatabase ignored, AtomicKVStore kvstore) {
        final XodusKVDatabase kvdb = new XodusKVDatabase();
        config.configure(kvdb);
        return kvdb;
    }

    @Override
    public String getDescription(Config config) {
        return "Xodus " + config.getDirectory().getName();
    }

// Options

    public static class Config {

        private File directory;
        private String storeName;

        public File getDirectory() {
            return this.directory;
        }
        public void setDirectory(File directory) {
            this.directory = directory;
        }

        public String getStoreName() {
            return this.storeName;
        }
        public void setStoreName(String storeName) {
            this.storeName = storeName;
        }

        public void configure(XodusKVDatabase kvdb) {
            Preconditions.checkArgument(this.directory != null, "Xodus directory must be specified via the `--xodus' flag");
            kvdb.setDirectory(this.directory);
            if (this.storeName != null)
                kvdb.setStoreName(this.storeName);
        }
    }
}
