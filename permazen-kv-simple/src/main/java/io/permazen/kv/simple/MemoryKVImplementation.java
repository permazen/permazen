
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.mvcc.AtomicKVStore;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class MemoryKVImplementation extends AbstractSimpleKVImplementation<MemoryKVImplementation.Config> {

    private OptionSpec<Void> memoryOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.memoryOption == null, "duplicate option");
        this.memoryOption = parser.accepts("memory", "Use an initially empty, in-memory database");
        this.addSimpleOptions(parser, this.memoryOption, "memory");
    }

    @Override
    public Config buildConfig(OptionSet options) {
        if (!options.has(this.memoryOption))
            return null;
        final Config config = new Config();
        this.applySimpleOptions(options, config);
        return config;
    }

    @Override
    public MemoryKVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final MemoryKVDatabase memoryKV = new MemoryKVDatabase();
        config.applyTo(memoryKV);
        return memoryKV;
    }

    @Override
    public String getDescription(Config config) {
        return "Memory database";
    }

// Config

    public static class Config extends AbstractSimpleKVImplementation.Config {
    }
}
