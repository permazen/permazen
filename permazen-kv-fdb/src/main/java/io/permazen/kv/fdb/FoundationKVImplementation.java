
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.fdb;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.io.File;
import java.nio.charset.StandardCharsets;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.ValueConverter;

public class FoundationKVImplementation implements KVImplementation<FoundationKVImplementation.Config> {

    private OptionSpec<File> clusterFileOption;
    private OptionSpec<ByteData> keyPrefixOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.clusterFileOption == null, "duplicate option");
        Preconditions.checkState(this.keyPrefixOption == null, "duplicate option");
        this.clusterFileOption = parser.accepts("fdb", "Use FoundationDB key/value database with specified cluster file")
          .withRequiredArg()
          .describedAs("cluster-file")
          .ofType(File.class);
        this.keyPrefixOption = parser.accepts("fdb-prefix",
            "Specify FoundationDB key prefix (a hex string or UTF-8 bytes in string form)")
          .availableIf(this.clusterFileOption)
          .withRequiredArg()
          .describedAs("prefix-bytes")
          .withValuesConvertedBy(new PrefixConverter());
    }

    @Override
    public Config buildConfig(OptionSet options) {
        final File clusterFile = this.clusterFileOption.value(options);
        if (clusterFile == null)
            return null;
        if (!clusterFile.exists())
            throw new IllegalArgumentException(String.format("FDB cluster file \"%s\" does not exist", clusterFile));
        return new Config(clusterFile, this.keyPrefixOption.value(options));
    }

    @Override
    public boolean providesKVDatabase(Config config) {
        return true;
    }

    @Override
    public FoundationKVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final FoundationKVDatabase fdb = new FoundationKVDatabase();
        fdb.setClusterFilePath(config.getClusterFile().toString());
        fdb.setKeyPrefix(config.getPrefix());
        return fdb;
    }

    @Override
    public String getDescription(Config config) {
        String desc = "FoundationDB " + config.getClusterFile().getName();
        final ByteData prefix = config.getPrefix();
        if (prefix != null && !prefix.isEmpty())
            desc += " [0x" + ByteUtil.toString(prefix) + "]";
        return desc;
    }

// Config

    public static class Config {

        private File clusterFile;
        private ByteData prefix;

        public Config(File clusterFile, ByteData prefix) {
            Preconditions.checkArgument(clusterFile != null, "null clusterFile");
            Preconditions.checkArgument(prefix != null, "null prefix");
            this.clusterFile = clusterFile;
            this.prefix = prefix;
        }

        public File getClusterFile() {
            return this.clusterFile;
        }

        public ByteData getPrefix() {
            return this.prefix;
        }
    }

// PrefixConverter

    public static class PrefixConverter implements ValueConverter<ByteData> {

        @Override
        public ByteData convert(String value) {
            try {
                return ByteData.fromHex(value);
            } catch (IllegalArgumentException e) {
                return ByteData.of(value.getBytes(StandardCharsets.UTF_8));
            }
        }

        @Override
        public String revert(Object value) {
            return ((ByteData)value).toHex();
        }

        @Override
        public Class<ByteData> valueType() {
            return ByteData.class;
        }

        @Override
        public String valuePattern() {
            return "Hex string or characters to be encoded as UTF-8";
        }
    }
}
