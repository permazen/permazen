
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.MemoryAtomicKVStore;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class MemoryKVImplementation extends AbstractSimpleKVImplementation<MemoryKVImplementation.Config> {

    private OptionSpec<Void> memoryOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.memoryOption == null, "duplicate option");
        this.memoryOption = parser.accepts("memory", "Use an in-memory database (or key/value store)");
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
    public boolean providesKVDatabase(Config config) {
        return true;
    }

    @Override
    public boolean providesAtomicKVStore(Config config) {
        return true;
    }

    @Override
    public MemoryKVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final MemoryKVDatabase memoryKV = new MemoryKVDatabase();
        config.applyTo(memoryKV);
        return memoryKV;
    }

    @Override
    public MemoryAtomicKVStore createAtomicKVStore(Config config) {
        return new MemoryAtomicKVStore();
    }

    @Override
    public String getDescription(Config config) {
        return "Memory database";
    }

// Config

    public static class Config extends AbstractSimpleKVImplementation.Config {
    }
}
