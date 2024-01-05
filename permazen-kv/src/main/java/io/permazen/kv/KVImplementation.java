
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.reflect.TypeToken;

import io.permazen.kv.mvcc.AtomicKVDatabase;
import io.permazen.kv.mvcc.AtomicKVStore;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Descriptor for a provider of {@link KVDatabase} and/or {@link AtomicKVStore} implementations.
 *
 * <p>
 * Instances of this class provide information about how to configure and instantiate some
 * technology-specific implementation of the {@link KVDatabase} and/or {@link AtomicKVStore}
 * interfaces.
 *
 * @param <C> configuration object type
 */
public interface KVImplementation<C> {

    /**
     * Get the configuration object type for this instance.
     *
     * @return config type
     */
    @SuppressWarnings("unchecked")
    default Class<C> getConfigType() {
        return (Class<C>)TypeToken.of(this.getClass()).resolveType(KVImplementation.class.getTypeParameters()[0]).getRawType();
    }

    /**
     * Add implementation-specific command line options.
     *
     * <p>
     * All of the added options must be optional, because multiple key/value implementations will
     * be available when the command line is parsed, and the presence of implementation-specific
     * options determines which one(s) are chosen.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     */
    void addOptions(OptionParser parser);

    /**
     * Build a configuration from implementation-specific command line options, if any were given.
     *
     * <p>
     * If none of this implementation's options were provided, then this method must return null.
     *
     * @param options parsed command line options
     * @return corresponding configuration, or null if this implementation was not configured
     * @throws OptionException if invalid options were given
     * @throws IllegalArgumentException if invalid options were given
     * @throws IllegalArgumentException if {@code options} is null
     */
    C buildConfig(OptionSet options);

    /**
     * Determine whether this {@link KVImplementation} includes an implementation of the {@link AtomicKVStore} interface.
     *
     * <p>
     * The implementation in {@link KVImplementation} return false.
     *
     * @param config implementation configuration returned by {@link #buildConfig buildConfig()}
     * @return true if this instance can create an {@link AtomicKVStore}
     * @throws IllegalArgumentException if {@code config} is null
     */
    default boolean providesAtomicKVStore(C config) {
        return false;
    }

    /**
     * Determine whether this {@link KVImplementation} includes an implementation of the {@link KVDatabase} interface.
     *
     * <p>
     * The implementation in {@link KVImplementation} return false.
     *
     * @param config implementation configuration returned by {@link #buildConfig buildConfig()}
     * @return true if this instance can create an {@link KVDatabase}
     * @throws IllegalArgumentException if {@code config} is null
     */
    default boolean providesKVDatabase(C config) {
        return false;
    }

    /**
     * Create an {@link KVDatabase} using the specified configuration.
     *
     * <p>
     * The implementation in {@link KVImplementation} always throws {@link UnsupportedOperationException}.
     *
     * @param config implementation configuration returned by {@link #buildConfig buildConfig()}
     * @param kvdb required {@link KVDatabase}; will be null unless {@link #requiresKVDatabase} returned true
     * @param kvstore required {@link AtomicKVStore}; will be null unless {@link #requiresAtomicKVStore} returned true
     * @return new {@link KVDatabase} instance
     * @throws UnsupportedOperationException if {@link #providesKVDatabase} returns false
     * @throws IllegalArgumentException if {@code config} is null
     */
    default KVDatabase createKVDatabase(C config, KVDatabase kvdb, AtomicKVStore kvstore) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create an {@link AtomicKVStore} using the specified configuration.
     *
     * <p>
     * The implementation in {@link KVImplementation} invokes {@link #createKVDatabase createKVDatabase()} and constructs
     * an {@link AtomicKVDatabase} from the result. Implementations that natively support the {@link AtomicKVDatabase}
     * interface should override this method.
     *
     * @param config implementation configuration returned by {@link #buildConfig buildConfig()}
     * @return new {@link AtomicKVStore} instance
     * @throws IllegalArgumentException if {@code config} is null
     */
    default AtomicKVStore createAtomicKVStore(C config) {
        throw new UnsupportedOperationException();
    }

    /**
     * Determine whether this instance's {@link KVDatabase} implementation requires an underlying {@link AtomicKVStore}.
     *
     * <p>
     * This method and {@link #requiresKVDatabase requiresKVDatabase()} may not both return true.
     *
     * <p>
     * The implementation in {@link KVImplementation} return false.
     *
     * @param config implementation configuration returned by {@link #buildConfig buildConfig()}
     * @return true if the implementation relies on an underlying {@link AtomicKVStore}
     * @throws IllegalArgumentException if {@code config} is null
     */
    default boolean requiresAtomicKVStore(C config) {
        return false;
    }

    /**
     * Determine whether this instance's {@link KVDatabase} implementation requires some other underlying {@link KVDatabase}.
     *
     * <p>
     * This method and {@link #requiresAtomicKVStore requiresAtomicKVStore()} may not both return true.
     *
     * <p>
     * The implementation in {@link KVImplementation} return false.
     *
     * @param config implementation configuration returned by {@link #buildConfig buildConfig()}
     * @return true if the implementation relies on an underlying {@link KVDatabase}
     * @throws IllegalArgumentException if {@code config} is null
     */
    default boolean requiresKVDatabase(C config) {
        return false;
    }

    /**
     * Generate a short, human-readable description of the {@link KVDatabase} instance configured as given.
     *
     * @param config implementation configuration returned by {@link #buildConfig buildConfig()}
     * @return human-readable description
     */
    String getDescription(C config);
}
