
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.simple;

import java.util.ArrayDeque;
import java.util.Iterator;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVImplementation;
import org.jsimpledb.kv.mvcc.AtomicKVStore;

public class MemoryKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--mem", "Use an initially empty, in-memory database" }
        };
    }

    @Override
    public Object parseCommandLineOptions(ArrayDeque<String> options) {
        Object config = null;
        for (Iterator<String> i = options.iterator(); i.hasNext(); ) {
            final String option = i.next();
            if (option.equals("--mem")) {
                config = Boolean.TRUE;
                i.remove();
            }
        }
        return config;
    }

    @Override
    public SimpleKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        return new SimpleKVDatabase();
    }

    @Override
    public String getDescription(Object configuration) {
        return "Memory database";
    }
}
