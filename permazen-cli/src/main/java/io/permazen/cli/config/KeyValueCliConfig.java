
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.config;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.ServiceLoader;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Configuration for a CLI application supporting key/value database interaction.
 */
public class KeyValueCliConfig extends CliConfig {

    // Options
    protected final ArrayList<KVImpl<?>> kvis = new ArrayList<>();

    // Database
    protected KVDatabase kvdb;
    protected String kvdbDescription;

    // Internal State
    protected KVImpl<?> kvi;
    protected AtomicKVStore nestedKVStore;
    protected KVDatabase nestedKVDatabase;

// Options

    /**
     * Configure an {@link OptionParser} with the comand line flags supported by this instance.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     */
    public void addOptions(OptionParser parser) {
        super.addOptions(parser);
        this.addKeyValueImplementationOptions(parser);
    }

    /**
     * Add key/value database implementation command line options.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     * @see KVImplementation
     */
    protected void addKeyValueImplementationOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        ServiceLoader.load(KVImplementation.class, this.loader).stream()
          .map(ServiceLoader.Provider::get)
          .map(impl -> (KVImplementation<?>)impl)
          .forEach(impl -> this.addImpl(parser, impl));
    }

    private <C> void addImpl(OptionParser parser, KVImplementation<C> impl) {
        impl.addOptions(parser);
        this.kvis.add(new KVImpl<>(impl));
    }

    @Override
    protected void processOptions(OptionSet options) {
        super.processOptions(options);

        // Apply options to key/value implementation(s) and remove those that were not selected
        this.kvis.removeIf(kvi -> !kvi.configure(options));

        // Sort those that remain
        this.kvis.sort(Comparator.comparingDouble(KVImpl::sortPoints));

        // Check result
        switch (this.kvis.size()) {
        case 0:
            throw new IllegalArgumentException(String.format("no key/value database specified"));
        case 1:
            this.kvi = this.kvis.get(0);
            final String requirement = this.kvi.requiresAtomicKVStore() ? "store" :
              this.kvi.requiresKVDatabase() ? "database" : null;
            if (requirement != null) {
                throw new IllegalArgumentException(String.format(
                  "%s requires the configuration of an underlying key/value %s", this.kvi.describe(), requirement));
            }
            break;

        case 2:
        {
            final KVImpl<?> kvi1 = this.kvis.get(0);
            final KVImpl<?> kvi2 = this.kvis.get(1);
            if (kvi1.requiresAtomicKVStore()
              || kvi1.requiresKVDatabase()
              || !kvi1.providesAtomicKVStore()
              || kvi2.requiresKVDatabase()
              || !kvi2.providesKVDatabase()
              || !kvi2.requiresAtomicKVStore()) {
                throw new IllegalArgumentException(String.format(
                  "incompatible combination of %s and %s", kvi1.describe(), kvi2.describe()));
            }
            this.nestedKVStore = kvi1.createAtomicKVStore();
            this.kvi = kvi2;
            break;
        }

        case 3:
        {
            final KVImpl<?> kvi1 = this.kvis.get(0);
            final KVImpl<?> kvi2 = this.kvis.get(1);
            final KVImpl<?> kvi3 = this.kvis.get(2);
            if (kvi1.requiresAtomicKVStore()
              || kvi1.requiresKVDatabase()
              || !kvi1.providesAtomicKVStore()
              || kvi2.requiresKVDatabase()
              || kvi2.requiresAtomicKVStore()
              || !kvi2.providesKVDatabase()
              || !kvi3.requiresAtomicKVStore()
              || !kvi3.requiresKVDatabase()
              || !kvi3.providesKVDatabase()) {
                throw new IllegalArgumentException(String.format(
                  "incompatible combination of %s, %s, and %s", kvi1.describe(), kvi2.describe(), kvi3.describe()));
            }
            this.nestedKVStore = kvi1.createAtomicKVStore();
            this.nestedKVDatabase = kvi2.createKVDatabase(null, null);
            this.kvi = kvi3;
            break;
        }

        default:
            throw new IllegalArgumentException("too many key/value store(s) specified");
        }
    }

// Database

    @Override
    public void startupDatabase(OptionSet options) {

        // Sanity check
        Preconditions.checkArgument(options != null, "null options");
        Preconditions.checkState(this.kvdb == null, "already started");

        // Create key/value database, including an underlying database/store as needed
        this.kvdb = this.kvi.createKVDatabase(this.nestedKVDatabase, this.nestedKVStore);
        this.kvdbDescription = this.kvi.describe();

        // Start up database
        this.log.debug("using database: {}", this.kvdbDescription);
        this.kvdb.start();
    }

    @Override
    public void shutdownDatabase() {
        if (this.kvdb != null) {
            this.kvdb.stop();
            this.kvdb = null;
        }
    }

    @Override
    public String getDatabaseDescription() {
        return this.kvdbDescription;
    }

    @Override
    public KVDatabase getKVDatabase() {
        return this.kvdb;
    }

// Session

    @Override
    protected void configureSession(Session session) {
        super.configureSession(session);                // nothing else to do here
    }

// KVImpl

    private static class KVImpl<C> {

        private final KVImplementation<C> kvi;

        private C config;

        KVImpl(KVImplementation<C> kvi) {
            this.kvi = kvi;
        }

        public boolean configure(OptionSet options) {
            final Object configObject = this.kvi.buildConfig(options);
            if (configObject == null)
                return false;
            this.config = this.kvi.getConfigType().cast(configObject);
            return true;
        }

        public boolean providesAtomicKVStore() {
            return this.kvi.providesAtomicKVStore(this.config);
        }

        public boolean providesKVDatabase() {
            return this.kvi.providesKVDatabase(this.config);
        }

        public boolean requiresAtomicKVStore() {
            return this.kvi.requiresAtomicKVStore(this.config);
        }

        public boolean requiresKVDatabase() {
            return this.kvi.requiresKVDatabase(this.config);
        }

        public String describe() {
            return this.kvi.getDescription(this.config);
        }

        public AtomicKVStore createAtomicKVStore() {
            return this.kvi.createAtomicKVStore(this.config);
        }

        public KVDatabase createKVDatabase(KVDatabase kvdb, AtomicKVStore kvstore) {
            return this.kvi.createKVDatabase(this.config, kvdb, kvstore);
        }

        public double sortPoints() {
            double points = 0.0;
            if (this.requiresKVDatabase())
                points += 2;
            else if (this.requiresAtomicKVStore())
                points += 1;
            if (!this.providesAtomicKVStore())
                points += 0.5;
            return points;
        }
    }
}
