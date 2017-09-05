
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import java.io.File;
import java.util.ArrayDeque;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

public class XMLKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--xml file", "Use the specified XML flat file key/value database" }
        };
    }

    @Override
    public File parseCommandLineOptions(ArrayDeque<String> options) {
        final String arg = this.parseCommandLineOption(options, "--xml");
        return arg != null ? new File(arg) : null;
    }

    @Override
    public XMLKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        return new XMLKVDatabase((File)configuration);
    }

    @Override
    public String getDescription(Object configuration) {
        return "XML DB " + ((File)configuration).getName();
    }
}
