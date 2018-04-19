
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayDeque;

public class XMLKVImplementation extends KVImplementation<File> {

    public XMLKVImplementation() {
        super(File.class);
    }

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
    public XMLKVDatabase createKVDatabase(File file, KVDatabase kvdb, AtomicKVStore kvstore) {
        return new XMLKVDatabase(file);
    }

    @Override
    public String getDescription(File file) {
        return "XML DB " + file.getName();
    }
}
