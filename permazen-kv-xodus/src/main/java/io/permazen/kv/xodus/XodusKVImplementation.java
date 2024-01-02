
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.xodus;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class XodusKVImplementation implements KVImplementation<XodusKVImplementation.Config> {

    private OptionSpec<File> directoryOption;
    private OptionSpec<String> storeNameOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.directoryOption == null, "duplicate option");
        Preconditions.checkState(this.storeNameOption == null, "duplicate option");
        this.directoryOption = parser.accepts("xodus", "Use Xodus key/value database in the specified directory")
          .withRequiredArg()
          .describedAs("directory")
          .ofType(File.class);
        this.storeNameOption = parser.accepts("xodus-store",
            String.format("Specify Xodus store name (default \"%s\")", XodusKVDatabase.DEFAULT_STORE_NAME))
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("store-name");
    }

    @Override
    public Config buildConfig(OptionSet options) {
        final File dir = options.valueOf(this.directoryOption);
        if (dir == null)
            return null;
        if (dir.exists() && !dir.isDirectory())
            throw new IllegalArgumentException(String.format("file \"%s\" is not a directory", dir));
        final Config config = new Config(dir);
        config.setStoreName(options.valueOf(this.storeNameOption));
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

        public Config(File directory) {
            Preconditions.checkArgument(directory != null, "null directory");
            this.directory = directory;
        }

        public File getDirectory() {
            return this.directory;
        }

        public String getStoreName() {
            return this.storeName;
        }
        public void setStoreName(String storeName) {
            this.storeName = storeName;
        }

        public void configure(XodusKVDatabase kvdb) {
            kvdb.setDirectory(this.directory);
            if (this.storeName != null)
                kvdb.setStoreName(this.storeName);
        }
    }
}
