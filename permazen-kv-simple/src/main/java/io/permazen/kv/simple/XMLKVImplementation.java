
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class XMLKVImplementation implements KVImplementation<File> {

    private OptionSpec<File> fileOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.fileOption == null, "duplicate option");
        this.fileOption = parser.accepts("xml", "Use the specified XML flat file key/value database")
          .withRequiredArg()
          .describedAs("file")
          .ofType(File.class);
    }

    @Override
    public File buildConfig(OptionSet options) {
        final File file = this.fileOption.value(options);
        if (file == null)
            return null;
        if (file.exists() && !file.isFile())
            throw new IllegalArgumentException(String.format("file \"%s\" is not a regular file", file));
        return file;
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
