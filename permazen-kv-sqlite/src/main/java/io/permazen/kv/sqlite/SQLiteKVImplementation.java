
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.sqlite;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.io.File;
import java.util.ArrayList;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class SQLiteKVImplementation implements KVImplementation<SQLiteKVImplementation.Config> {

    private OptionSpec<File> fileOption;
    private OptionSpec<Void> exclusiveOption;
    private OptionSpec<String> pragmaOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.fileOption == null, "duplicate option");
        Preconditions.checkState(this.exclusiveOption == null, "duplicate option");
        Preconditions.checkState(this.pragmaOption == null, "duplicate option");
        this.fileOption = parser.accepts("sqlite", "Use SQLite key/value database using the specified file")
          .withRequiredArg()
          .describedAs("file")
          .ofType(File.class);
        this.exclusiveOption = parser.accepts("sqlite-exclusive", "Configure SQLite connections for exclusive locking")
          .availableIf(this.fileOption);
        this.pragmaOption = parser.accepts("sqlite-pragma",
            "Specify a PRAGMA for new connections (omit PRAGMA keyword; may be repeated)")
          .availableIf(this.fileOption)
          .withRequiredArg()
          .describedAs("pragma");
    }

    @Override
    public Config buildConfig(OptionSet options) {
        final File file = this.fileOption.value(options);
        if (file == null)
            return null;
        if (file.exists() && !file.isFile())
            throw new IllegalArgumentException(String.format("file \"%s\" is not a regular file", file));
        final Config config = new Config(file);
        config.setExclusiveLocking(options.has(this.exclusiveOption));
        config.getPragmas().addAll(options.valuesOf(this.pragmaOption));
        return config;
    }

    @Override
    public SQLiteKVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final SQLiteKVDatabase sqlite = new SQLiteKVDatabase();
        config.configure(sqlite);
        return sqlite;
    }

    @Override
    public String getDescription(Config config) {
        return "SQLite " + config.getFile().getName();
    }

// Config

    public static class Config {

        private final File file;

        private boolean exclusiveLocking;
        private final ArrayList<String> pragmas = new ArrayList<>();

        public Config(File file) {
            Preconditions.checkArgument(file != null, "null file");
            this.file = file;
        }

        public File getFile() {
            return this.file;
        }

        public boolean isExclusiveLocking() {
            return this.exclusiveLocking;
        }
        public void setExclusiveLocking(boolean exclusiveLocking) {
            this.exclusiveLocking = exclusiveLocking;
        }

        public ArrayList<String> getPragmas() {
            return this.pragmas;
        }

        public void configure(SQLiteKVDatabase sqlite) {
            Preconditions.checkArgument(sqlite != null, "null sqlite");
            sqlite.setDatabaseFile(this.file);
            sqlite.setExclusiveLocking(this.exclusiveLocking);
            sqlite.setPragmas(this.pragmas);
        }
    }
}
