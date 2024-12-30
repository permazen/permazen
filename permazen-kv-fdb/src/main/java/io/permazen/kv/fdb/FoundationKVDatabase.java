
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.fdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.NetworkOptions;
import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVDatabaseException;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * FoundationDB {@link KVDatabase} implementation.
 *
 * <p>
 * Allows specifying a {@linkplain #setKeyPrefix key prefix} for all keys, allowing multiple independent databases.
 * {@linkplain FoundationKVTransaction#watchKey Key watches} are supported.
 *
 * @see <a href="https://www.foundationdb.org/">FoundationDB</a>
 */
public class FoundationKVDatabase implements KVDatabase {

    /**
     * The API version used by this class.
     */
    public static final int API_VERSION = 510;

    private final FDB fdb = FDB.selectAPIVersion(API_VERSION);
    private final NetworkOptions options = this.fdb.options();

    private String clusterFilePath;
    private ByteData keyPrefix;
    private Executor executor;

    private Database database;
    private boolean started;                                // FDB can only be started up once

    /**
     * Constructor.
     *
     * @throws FDBException if {@link #API_VERSION} is not supported
     */
    public FoundationKVDatabase() {
    }

    /**
     * Get the {@link NetworkOptions} associated with this instance.
     * Options must be configured prior to {@link #start}.
     *
     * @return network options
     */
    public NetworkOptions getNetworkOptions() {
        return this.options;
    }

    /**
     * Configure the {@link Executor} used for the FoundationDB networking event loop.
     *
     * <p>
     * By default, the default thread pool is used to execute the FoundationDB network.
     *
     * @param executor executor for networking activity
     * @see FDB#startNetwork(Executor) FDB.startNetwork()
     */
    public synchronized void setExecutor(Executor executor) {
        this.executor = executor;
    }

    /**
     * Configure the cluster file path. Default is null, which results in the default fdb.cluster file being used.
     *
     * @param clusterFilePath cluster file pathname
     */
    public synchronized void setClusterFilePath(String clusterFilePath) {
        this.clusterFilePath = clusterFilePath;
    }

    /**
     * Get the key prefix for all keys.
     *
     * @return key prefix, or null if there is none configured
     */
    public synchronized ByteData getKeyPrefix() {
        return this.keyPrefix;
    }

    /**
     * Configure a prefix for all keys.
     *
     * <p>
     * The prefix will be added/removed automatically with all access.
     * By default there is no prefix.
     *
     * <p>
     * The key prefix may not be changed after this instance has {@linkplain #start started}.
     *
     * @param keyPrefix new prefix, or null or empty for none
     * @throws IllegalArgumentException if {@code keyPrefix} starts with {@code 0xff}
     * @throws IllegalStateException if this instance has already been {@linkplain #start started}
     */
    public synchronized void setKeyPrefix(ByteData keyPrefix) {
        Preconditions.checkState(this.database == null, "already started");
        Preconditions.checkArgument(keyPrefix == null || !keyPrefix.startsWith(FoundationKVStore.MAX_KEY),
          "prefix starts with 0xff");
        if (keyPrefix != null && keyPrefix.isEmpty())
            keyPrefix = null;
        this.keyPrefix = keyPrefix;
    }

    /**
     * Get the underlying {@link Database} associated with this instance.
     *
     * @return the associated {@link Database}
     * @throws IllegalStateException if this instance has not yet been {@linkplain #start started}
     */
    public synchronized Database getDatabase() {
        Preconditions.checkState(this.database != null, "not started");
        return this.database;
    }

// KVDatabase

    @Override
    @PostConstruct
    public synchronized void start() {
        if (this.database != null)
            return;
        if (this.started)
            throw new UnsupportedOperationException("restarts not supported");
        this.database = this.fdb.open(this.clusterFilePath);
        if (this.executor != null)
            this.fdb.startNetwork(this.executor);
        else
            this.fdb.startNetwork();
        this.started = true;
    }

    @Override
    @PreDestroy
    public synchronized void stop() {
        if (this.database == null)
            return;
        this.fdb.stopNetwork();
        this.database = null;
    }

    @Override
    public FoundationKVTransaction createTransaction() {
        return this.createTransaction(null);
    }

    @Override
    public synchronized FoundationKVTransaction createTransaction(Map<String, ?> options) {
        Preconditions.checkState(this.database != null, "not started");
        try {
            return new FoundationKVTransaction(this, this.database.createTransaction(), this.keyPrefix);
        } catch (FDBException e) {
            throw new KVDatabaseException(this, e);
        }
    }

// Counters

    /**
     * Encode a 64 bit counter value.
     *
     * @param value counter value
     * @return encoded value
     */
    public static ByteData encodeCounter(long value) {
        final ByteData.Writer writer = ByteData.newWriter(8);
        ByteUtil.writeLong(writer, value);
        return FoundationKVDatabase.reverse(writer.toByteData());
    }

    /**
     * Decode a 64 bit counter value.
     *
     * @param bytes encoded value
     * @return value counter value
     * @throws NullPointerException if {@code bytes} is null
     * @throws IllegalArgumentException if {@code bytes} is invalid
     */
    public static long decodeCounter(ByteData bytes) {
        Preconditions.checkArgument(bytes.size() == 8, "invalid encoded counter value length != 8");
        return ByteUtil.readLong(FoundationKVDatabase.reverse(bytes).newReader());
    }

    private static ByteData reverse(ByteData bytes) {
        assert bytes != null && bytes.size() == 8;
        final byte[] array = bytes.toByteArray();
        int i = 0;
        int j = array.length - 1;
        while (i < j) {
            final byte temp = array[i];
            array[i] = array[j];
            array[j] = temp;
            i++;
            j--;
        }
        return ByteData.of(array);
    }
}
