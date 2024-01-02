
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.Optional;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.h2.mvstore.MVStore;

public class MVStoreKVImplementation implements KVImplementation<MVStoreKVImplementation.Config> {

    /**
     * Default MVStore map name to use ({@value #DEFAULT_MAP_NAME}).
     */
    public static final String DEFAULT_MAP_NAME = "Permazen";

    private OptionSpec<File> fileOption;
    private OptionSpec<Void> memoryOption;
    private OptionSpec<Void> readOnlyOption;
    private OptionSpec<Void> compressLzfOption;
    private OptionSpec<Void> compressDeflateOption;
    private OptionSpec<String> encryptionKeyOption;
    private OptionSpec<String> mapNameOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.fileOption == null, "duplicate option");
        Preconditions.checkState(this.memoryOption == null, "duplicate option");
        Preconditions.checkState(this.readOnlyOption == null, "duplicate option");
        Preconditions.checkState(this.compressLzfOption == null, "duplicate option");
        Preconditions.checkState(this.compressDeflateOption == null, "duplicate option");
        Preconditions.checkState(this.encryptionKeyOption == null, "duplicate option");
        Preconditions.checkState(this.mapNameOption == null, "duplicate option");
        this.fileOption = parser.accepts("mvstore", "Use MVStore key/value database using the specified file")
          .withRequiredArg()
          .describedAs("file")
          .ofType(File.class);
        this.memoryOption = parser.accepts("mvstore-memory", "Use MVStore key/value database with in-memory storage")
          .availableUnless(this.fileOption);
        this.readOnlyOption = parser.accepts("mvstore-read-only", "Open MVStore key/value database read-only")
          .availableIf(this.fileOption, this.memoryOption);
        this.compressLzfOption = parser.accepts("mvstore-compress-lzf", "Enable MVStore LZF compression")
          .availableIf(this.fileOption, this.memoryOption);
        this.compressDeflateOption = parser.accepts("mvstore-compress-deflate", "Enable MVStore Deflate compression")
          .availableIf(this.fileOption, this.memoryOption)
          .availableUnless(this.compressLzfOption);
        this.encryptionKeyOption = parser.accepts("mvstore-encrypt", "Enable MVStore encryption using the specified key")
          .availableIf(this.fileOption, this.memoryOption)
          .withRequiredArg()
          .describedAs("key");
        this.mapNameOption = parser.accepts("mvstore-map",
            String.format("Specify MVStore map name (default \"%s\")", DEFAULT_MAP_NAME))
          .availableIf(this.fileOption, this.memoryOption)
          .withRequiredArg()
          .describedAs("map-name")
          .defaultsTo(DEFAULT_MAP_NAME);
    }

    @Override
    public Config buildConfig(OptionSet options) {
        final Config config = new Config();
        final File file = this.fileOption.value(options);
        if (file != null)
            config.setFile(file);
        else if (options.has(this.memoryOption))
            config.setMemory(true);
        else
            return null;
        config.setReadOnly(options.has(this.readOnlyOption));
        config.setCompress(options.has(this.compressLzfOption));
        config.setCompressHigh(options.has(this.compressDeflateOption));
        config.setEncryptKey(this.encryptionKeyOption.value(options));
        config.setMapName(this.mapNameOption.value(options));
        return config;
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
    }
}
