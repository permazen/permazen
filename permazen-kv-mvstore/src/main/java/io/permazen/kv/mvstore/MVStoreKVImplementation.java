
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Optional;

import org.h2.mvstore.MVStore;

public class MVStoreKVImplementation extends KVImplementation<MVStoreKVImplementation.Config> {

    /**
     * Default MVStore map name to use ({@value #DEFAULT_MAP_NAME}).
     */
    public static final String DEFAULT_MAP_NAME = "Permazen";

    public MVStoreKVImplementation() {
        super(Config.class);
    }

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--mvstore file",
              "Use MVStore key/value database using the specified file" },
            { "--mvstore-memory",
              "Use MVStore key/value database with non-persistent in-memory storage" },
            { "--mvstore-read-only",
              "Open MVStore key/value database read-only" },
            { "--mvstore-compress-lzf",
              "Enable MVStore LZF compression" },
            { "--mvstore-compress-deflate",
              "Enable MVStore Deflate compression" },
            { "--mvstore-encrypt key",
              "Enable MVStore encryption using the specified key" },
            { "--mvstore-map name",
              "Specify MVStore map name (default \"" + DEFAULT_MAP_NAME + "\")" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        final Config config = new Config();
        config.setFile(Optional.ofNullable(this.parseCommandLineOption(options, "--mvstore")).map(File::new).orElse(null));
        config.setMemory(this.parseCommandLineFlag(options, "--mvstore-memory"));
        config.setReadOnly(this.parseCommandLineFlag(options, "--mvstore-read-only"));
        config.setCompress(this.parseCommandLineFlag(options, "--mvstore-compress-lzf"));
        config.setCompressHigh(this.parseCommandLineFlag(options, "--mvstore-compress-deflate"));
        config.setEncryptKey(this.parseCommandLineOption(options, "--mvstore-encrypt"));
        config.setMapName(Optional.ofNullable(this.parseCommandLineOption(options, "--mvstore-map")).orElse(DEFAULT_MAP_NAME));
        return config.isEnabled() ? config.validate() : null;
    }

    @Override
    public MVStoreKVDatabase createKVDatabase(Config config, KVDatabase ignored1, AtomicKVStore ignored2) {
        final MVStoreKVDatabase kvdb = new MVStoreKVDatabase();
        kvdb.setKVStore(this.createAtomicKVStore(config));
        return kvdb;
    }

    @Override
    public MVStoreAtomicKVStore createAtomicKVStore(Config config) {
        return config.configure(new MVStoreAtomicKVStore());
    }

    @Override
    public String getDescription(Config config) {
        return "MVStore " + Optional.ofNullable(config.getFile()).map(File::getName).orElse("[Memory]");
    }

// Config

    public static class Config {

        private File file;
        private boolean memory;
        private boolean readOnly;
        private boolean compress;
        private boolean compressHigh;
        private String encryptKey;
        private String mapName;

        public void setFile(final File file) {
            this.file = file;
        }
        public File getFile() {
            return this.file;
        }

        public void setMemory(final boolean memory) {
            this.memory = memory;
        }

        public void setReadOnly(final boolean readOnly) {
            this.readOnly = readOnly;
        }

        public void setCompress(final boolean compress) {
            this.compress = compress;
        }

        public void setCompressHigh(final boolean compressHigh) {
            this.compressHigh = compressHigh;
        }

        public void setEncryptKey(final String encryptKey) {
            this.encryptKey = encryptKey;
        }

        public void setMapName(String mapName) {
            this.mapName = mapName;
        }

        public boolean isEnabled() {
            return this.file != null || this.memory;
        }

        public MVStoreAtomicKVStore configure(MVStoreAtomicKVStore kvstore) {
            final MVStore.Builder builder = new MVStore.Builder();
            if (this.compress)
                builder.compress();
            if (this.compressHigh)
                builder.compressHigh();
            if (this.encryptKey != null)
                builder.encryptionKey(this.encryptKey.toCharArray());
            if (this.file != null)
                builder.fileName(this.file.toString());
            if (this.readOnly)
                builder.readOnly();
            kvstore.setBuilder(builder);
            kvstore.setMapName(this.mapName);
            return kvstore;
        }

        public Config validate() {
            if ((this.file != null) == this.memory)
                throw new IllegalArgumentException("exactly one of \"--mvstore\" or \"--mvstore-memory\" must be specified");
            if (this.compress && this.compressHigh) {
                throw new IllegalArgumentException("flags \"--mvstore-compress-lzf\" and \"--mvstore-compress-deflate\""
                  + " are incompatible");
            }
            return this;
        }
    }
}
