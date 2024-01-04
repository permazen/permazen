
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class XMLKVImplementation extends AbstractSimpleKVImplementation<XMLKVImplementation.Config> {

    private OptionSpec<File> xmlOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.xmlOption == null, "duplicate option");
        this.xmlOption = parser.accepts("xml", "Use the specified XML flat file key/value database")
          .withRequiredArg()
          .describedAs("file")
          .ofType(File.class);
        this.addSimpleOptions(parser, this.xmlOption, "xml");
    }

    @Override
    public Config buildConfig(OptionSet options) {
        final File file = this.xmlOption.value(options);
        if (file == null)
            return null;
        if (file.exists() && !file.isFile())
            throw new IllegalArgumentException(String.format("file \"%s\" is not a regular file", file));
        final Config config = new Config(file);
        this.applySimpleOptions(options, config);
        return config;
    }

    @Override
    public XMLKVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final XMLKVDatabase xmlKV = new XMLKVDatabase(config.getFile());
        config.applyTo(xmlKV);
        return xmlKV;
    }

    @Override
    public String getDescription(Config config) {
        return "XML DB " + config.getFile().getName();
    }

// Config

    public static class Config extends AbstractSimpleKVImplementation.Config {

        private File file;

        public Config(File file) {
            this.file = file;
        }

        public File getFile() {
            return this.file;
        }
    }
}
