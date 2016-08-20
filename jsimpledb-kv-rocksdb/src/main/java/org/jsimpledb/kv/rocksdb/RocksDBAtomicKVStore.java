
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.rocksdb;

import com.google.common.base.Preconditions;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.kv.util.ForwardingKVStore;
import org.jsimpledb.util.ByteUtil;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AtomicKVStore} view of a RocksDB database.
 *
 * <p>
 * A {@linkplain #setDirectory database directory} is the only required configuration property.
 * Instances may be stopped and (re)started multiple times.
 */
public class RocksDBAtomicKVStore extends ForwardingKVStore implements AtomicKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final AtomicBoolean shutdownHookRegistered = new AtomicBoolean();

    // Configuration
    private Options options;
    private File directory;

    // "Runtime" state
    private RocksDBKVStore kv;
    private RocksDB db;

// Constructors

    /**
     * Constructor.
     */
    public RocksDBAtomicKVStore() {
        this.setOptions(new Options().setCreateIfMissing(true));
    }

// Accessors

    /**
     * Get the filesystem directory containing the database.
     *
     * @return database directory
     */
    public synchronized File getDirectory() {
        return this.directory;
    }

    /**
     * Configure the filesystem directory containing the database. Required property.
     *
     * @param directory database directory
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setDirectory(File directory) {
        Preconditions.checkState(this.db == null, "already started");
        this.directory = directory;
    }

    /**
     * Get the underlying {@link RocksDB} associated with this instance.
     *
     * @return the associated {@link RocksDB}
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    public synchronized RocksDB getDB() {
        Preconditions.checkState(this.db != null, "not started");
        return this.db;
    }

// Options

    /**
     * Get the {@link Options} this instance will use when opening the database at startup.
     *
     * @return database options
     */
    public synchronized Options getOptions() {
        return this.options;
    }

    /**
     * Set the {@link Options} this instance will use when opening the database at startup.
     * Overwrites any previous options configuration(s).
     *
     * @param options database options
     * @throws IllegalArgumentException if {@code options} is null
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setOptions(Options options) {
        Preconditions.checkArgument(options != null, "null options");
        Preconditions.checkState(this.db == null, "already started");
        this.options = options;
        this.options.setMergeOperatorName("uint64add");
        this.setLogger(this.options);
    }

    private void setLogger(Options options) {
        Preconditions.checkArgument(options != null);
        options.setLogger(new org.rocksdb.Logger(options) {
            @Override
            protected void log(InfoLogLevel level, String message) {
                switch (level) {
                case DEBUG_LEVEL:
                    RocksDBAtomicKVStore.this.log.trace("[RocksDB] " + message);
                    break;
                case INFO_LEVEL:
                    RocksDBAtomicKVStore.this.log.info("[RocksDB] " + message);
                    break;
                case WARN_LEVEL:
                    RocksDBAtomicKVStore.this.log.warn("[RocksDB] " + message);
                    break;
                case ERROR_LEVEL:
                case FATAL_LEVEL:
                default:
                    RocksDBAtomicKVStore.this.log.error("[RocksDB] " + message);
                    break;
                }
            }
        });
    }

// Lifecycle

    @Override
    @PostConstruct
    public synchronized void start() {

        // Already started?
        if (this.db != null)
            return;
        this.log.info("starting " + this);

        // Check configuration
        Preconditions.checkState(this.directory != null, "no directory configured");

        // Create directory if needed
        if (!this.directory.exists()) {
            if (!this.options.createIfMissing())
                throw new RuntimeException("directory `" + this.directory + "' does not exist");
            if (!this.directory.mkdirs())
                throw new RuntimeException("failed to create directory `" + this.directory + "'");
        }
        if (!this.directory.isDirectory())
            throw new RuntimeException("file `" + this.directory + "' is not a directory");

        // Open database
        if (this.log.isDebugEnabled())
            this.log.debug("opening " + this + " RocksDB database");
        try {
            this.db = RocksDB.open(this.options, this.directory.toString());
        } catch (RocksDBException e) {
            throw new RuntimeException("RocksDB database startup failed", e);
        }

        // Create k/v store view
        this.kv = new RocksDBKVStore(this.db);

        // Add shutdown hook so we don't leak native resources
        if (this.shutdownHookRegistered.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    RocksDBAtomicKVStore.this.stop();
                }
            });
        }
    }

    @Override
    @PreDestroy
    public synchronized void stop() {

        // Check state
        if (this.db == null)
            return;
        this.log.info("stopping " + this);

        // Close k/v store view
        this.kv.close();
        this.kv = null;

        // Shut down RocksDB database
        try {
            if (this.log.isDebugEnabled())
                this.log.info("closing " + this + " RocksDB database");
            this.db.close();
        } catch (Throwable e) {
            this.log.error("caught exception closing database during shutdown (ignoring)", e);
        }
        this.db = null;
    }

// ForwardingKVStore

    @Override
    protected synchronized RocksDBKVStore delegate() {
        Preconditions.checkState(this.db != null && this.kv != null, "closed");
        return this.kv;
    }

// AtomicKVStore

    @Override
    public synchronized SnapshotRocksDBKVStore snapshot() {
        Preconditions.checkState(this.db != null, "closed");
        return new SnapshotRocksDBKVStore(this.db);
    }

    @Override
    public synchronized void mutate(Mutations mutations, boolean sync) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        Preconditions.checkState(this.db != null, "closed");

        // Apply mutations in a batch
        try (final WriteBatch batch = new WriteBatch()) {

            // Apply removes
            try (final ReadOptions iteratorOptions = new ReadOptions().setFillCache(false)) {
                for (KeyRange range : mutations.getRemoveRanges()) {
                    final byte[] min = range.getMin();
                    final byte[] max = range.getMax();
                    if (min != null && max != null && ByteUtil.isConsecutive(min, max))
                        batch.remove(min);
                    else {
                        try (RocksDBKVStore.Iterator i = this.kv.createIterator(iteratorOptions, min, max, false)) {
                            while (i.hasNext())
                                batch.remove(i.next().getKey());
                        }
                    }
                }
            }

            // Apply puts
            for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs())
                batch.put(entry.getKey(), entry.getValue());

            // Apply counter adjustments
            for (Map.Entry<byte[], Long> entry : mutations.getAdjustPairs())
                batch.merge(entry.getKey(), this.kv.encodeCounter(entry.getValue()));

            // Write the batch
            try (final WriteOptions writeOptions = new WriteOptions().setSync(sync)) {
                this.db.write(writeOptions, batch);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("error applying changes to RocksDB", e);
        }
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #stop} to close any unclosed iterators.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            if (this.db != null)
               this.log.warn(this + " leaked without invoking stop()");
            this.stop();
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[dir=" + this.directory
          + ",kv=" + this.kv
          + "]";
    }
}

