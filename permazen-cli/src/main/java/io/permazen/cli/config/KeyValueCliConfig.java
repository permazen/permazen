
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.config;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Configuration for a CLI application supporting key/value database interaction.
 */
public class KeyValueCliConfig extends CliConfig {

    // Options
    protected final HashMap<KVImplementation<?>, Object> kviOptions = new HashMap<>();

    // Database
    protected String kvdbDescription;
    protected KVDatabase kvdb;

    // Internal State
    private KVImplementation<?> kvi;
    private Object config;
    private AtomicKVStore nestedKVStore;
    private KVDatabase nestedKVDatabase;

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
          .forEach(kv -> {
            kv.addOptions(parser);
            this.kviOptions.put(kv, null);
        });
    }

    @Override
    protected void processOptions(OptionSet options) {
        super.processOptions(options);

        // Apply options to key/value implementation(s)
        for (Iterator<Map.Entry<KVImplementation<?>, Object>> i = kviOptions.entrySet().iterator(); i.hasNext(); ) {
            final Map.Entry<KVImplementation<?>, Object> entry = i.next();
            assert entry.getValue() == null;
            final KVImplementation<?> kv = entry.getKey();
            final Object kvConfig = kv.buildConfig(options);
            if (kvConfig != null)
                entry.setValue(kvConfig);           // this implementation got configured, so use it
            else
                i.remove();                         // this implementation was not configured, so discard it
        }

        // Decode what key/value implementations where specified and how they nest, if at all
        final Iterator<Map.Entry<KVImplementation<?>, Object>> i = kviOptions.entrySet().iterator();
        switch (this.kviOptions.size()) {
        case 0:
            throw new IllegalArgumentException(String.format("no key/value database specified"));
        case 1:
            final Map.Entry<KVImplementation<?>, Object> entry = i.next();
            kvi = entry.getKey();
            config = entry.getValue();
            if (this.requiresAtomicKVStore(kvi, config) || this.requiresKVDatabase(kvi, config)) {
                throw new IllegalArgumentException(String.format(
                  "%s requires the configuration of an underlying key/value technology", this.describe(kvi, config)));
            }
            break;

        case 2:

            // Put them in proper order: inner first, outer second
            final Map.Entry<KVImplementation<?>, Object> entry1 = i.next();
            final Map.Entry<KVImplementation<?>, Object> entry2 = i.next();
            final KVImplementation<?>[] kvis = new KVImplementation<?>[] { entry1.getKey(), entry2.getKey() };
            final Object[] configs = new Object[] { entry1.getValue(), entry2.getValue() };
            if (this.requiresAtomicKVStore(kvis[0], configs[0]) || this.requiresKVDatabase(kvis[0], configs[0])) {
                Collections.reverse(Arrays.asList(kvis));
                Collections.reverse(Arrays.asList(configs));
            }

            // Sanity check nesting requirements
            if ((this.requiresAtomicKVStore(kvis[0], configs[0]) || this.requiresKVDatabase(kvis[0], configs[0]))
              || !(this.requiresAtomicKVStore(kvis[1], configs[1]) || this.requiresKVDatabase(kvis[1], configs[1]))) {
                throw new IllegalArgumentException(String.format("incompatible combination of %s and %s",
                  this.describe(kvis[0], configs[0]), this.describe(kvis[1], configs[1])));
            }

            // Nest them as required
            if (this.requiresAtomicKVStore(kvis[1], configs[1]))
                this.nestedKVStore = this.createAtomicKVStore(kvis[0], configs[0]);
            else
                this.nestedKVDatabase = this.createKVDatabase(kvis[0], configs[0], null, null);
            this.kvi = kvis[1];
            this.config = configs[1];
            break;

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
        this.kvdb = this.createKVDatabase(this.kvi, this.config, this.nestedKVDatabase, this.nestedKVStore);
        this.kvdbDescription = this.describe(this.kvi, this.config);

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

// Internal Methods

    // Generic type futzing
    private <C> boolean requiresAtomicKVStore(KVImplementation<C> kvi, Object config) {
        return kvi.requiresAtomicKVStore(kvi.getConfigType().cast(config));
    }
    private <C> boolean requiresKVDatabase(KVImplementation<C> kvi, Object config) {
        return kvi.requiresKVDatabase(kvi.getConfigType().cast(config));
    }
    private <C> String describe(KVImplementation<C> kvi, Object config) {
        return kvi.getDescription(kvi.getConfigType().cast(config));
    }
    private <C> KVDatabase createKVDatabase(KVImplementation<C> kvi, Object config, KVDatabase kvdb, AtomicKVStore kvstore) {
        return kvi.createKVDatabase(kvi.getConfigType().cast(config), kvdb, kvstore);
    }
    private <C> AtomicKVStore createAtomicKVStore(KVImplementation<C> kvi, Object config) {
        return kvi.createAtomicKVStore(kvi.getConfigType().cast(config));
    }
}
