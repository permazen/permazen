
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class MemoryKVImplementation implements KVImplementation<Boolean> {

    private OptionSpec<Void> memOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.memOption == null, "duplicate option");
        this.memOption = parser.accepts("mem", "Use an initially empty, in-memory database");
    }

    @Override
    public Boolean buildConfig(OptionSet options) {
        return options.has(this.memOption) ? true : null;
    }

    @Override
    public MemoryKVDatabase createKVDatabase(Boolean config, KVDatabase kvdb, AtomicKVStore kvstore) {
        return new MemoryKVDatabase();
    }

    @Override
    public String getDescription(Boolean config) {
        return "Memory database";
    }
}
