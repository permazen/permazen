
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

public class SimpleKVImplementation extends AbstractSimpleKVImplementation<SimpleKVImplementation.Config> {

    private OptionSpec<Void> simpleOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.simpleOption == null, "duplicate option");
        this.simpleOption = parser.accepts("simple", "Use a simple locking key/value database (requires key/value store)");
        this.addSimpleOptions(parser, this.simpleOption, "simple");
    }

    @Override
    public Config buildConfig(OptionSet options) {
        if (!options.has(this.simpleOption))
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
    public KVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final SimpleKVDatabase simpleKV = new SimpleKVDatabase(kvstore);
        config.applyTo(simpleKV);
        return simpleKV;
    }

    @Override
    public boolean requiresAtomicKVStore(Config config) {
        return true;
    }

    @Override
    public String getDescription(Config config) {
        return "Simple Database";
    }

// Config

    public static class Config extends AbstractSimpleKVImplementation.Config {
    }
}
