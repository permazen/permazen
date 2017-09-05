
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.fdb;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.util.ByteUtil;

public class FoundationKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
          { "--fdb file",
            "Use FoundationDB key/value database with specified cluster file" },
          { "--fdb-prefix prefix",
            "Specify FoundationDB key prefix (a hex string, otherwise UTF-8 encoded)" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        final String clusterFile = this.parseCommandLineOption(options, "--fdb");
        if (clusterFile == null)
            return null;
        if (!new File(clusterFile).exists())
            throw new IllegalArgumentException("file `" + clusterFile + "' does not exist");
        final Config config = new Config(clusterFile);
        final String prefix = this.parseCommandLineOption(options, "--fdb-prefix");
        if (prefix != null) {
            try {
                config.setPrefix(ByteUtil.parse(prefix));
            } catch (IllegalArgumentException e) {
                config.setPrefix(prefix.getBytes(StandardCharsets.UTF_8));
            }
        }
        return config;
    }

    @Override
    public FoundationKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        final Config config = (Config)configuration;
        final FoundationKVDatabase fdb = new FoundationKVDatabase();
        fdb.setClusterFilePath(config.getClusterFile());
        fdb.setKeyPrefix(config.getPrefix());
        return fdb;
    }

    @Override
    public String getDescription(Object configuration) {
        final Config config = (Config)configuration;
        String desc = "FoundationDB " + new File(config.getClusterFile()).getName();
        final byte[] prefix = config.getPrefix();
        if (prefix != null && prefix.length > 0)
            desc += " [0x" + ByteUtil.toString(prefix) + "]";
        return desc;
    }

// Config

    private static class Config {

        private String clusterFile;
        private byte[] prefix;

        Config(String clusterFile) {
            if (clusterFile == null)
                throw new IllegalArgumentException("null clusterFile");
            this.clusterFile = clusterFile;
        }

        public String getClusterFile() {
            return this.clusterFile;
        }

        public byte[] getPrefix() {
            return this.prefix;
        }

        public void setPrefix(byte[] prefix) {
            this.prefix = prefix;
        }
    }
}
