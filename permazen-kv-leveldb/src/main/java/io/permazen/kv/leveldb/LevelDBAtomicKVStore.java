
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import com.google.common.base.Preconditions;

import io.permazen.kv.KeyRange;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AtomicKVStore} view of a LevelDB database.
 *
 * <p>
 * A {@linkplain #setDirectory database directory} is the only required configuration property.
 * Instances may be stopped and (re)started multiple times.
 */
@ThreadSafe
public class LevelDBAtomicKVStore extends ForwardingKVStore implements AtomicKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final AtomicBoolean shutdownHookRegistered = new AtomicBoolean();
    private final DBFactory factory;

    @GuardedBy("this")
    private Options options = new Options().createIfMissing(true).logger(new org.iq80.leveldb.Logger() {
        @Override
        public void log(String message) {
            LevelDBAtomicKVStore.this.log.info("[LevelDB] {}", message);
        }
      });
    @GuardedBy("this")
    private File directory;

    @GuardedBy("this")
    private LevelDBKVStore kv;
    @GuardedBy("this")
    private DB db;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Uses the default {@link DBFactory} provided by {@link LevelDBUtil#getDefaultDBFactory}.
     */
    public LevelDBAtomicKVStore() {
        this(LevelDBUtil.getDefaultDBFactory());
    }

    /**
     * Constructor.
     *
     * @param factory factory for database
     * @throws IllegalArgumentException if {@code factory} is null
     */
    public LevelDBAtomicKVStore(DBFactory factory) {
        Preconditions.checkArgument(factory != null, "null factory");
        this.factory = factory;
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
     * Get the underlying {@link DB} associated with this instance.
     *
     * @return the associated {@link DB}
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    public synchronized DB getDB() {
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
    }

    /**
     * Configure the number of keys between restart points for delta encoding of keys. Default 16.
     *
     * @param blockRestartInterval restart interval
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setBlockRestartInterval(int blockRestartInterval) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.blockRestartInterval(blockRestartInterval);
    }

    /**
     * Configure the block size. Default 4K.
     *
     * @param blockSize block size
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setBlockSize(int blockSize) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.blockSize(blockSize);
    }

    /**
     * Configure the cache size. Default 8MB.
     *
     * @param cacheSize cache size
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setCacheSize(long cacheSize) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.cacheSize(cacheSize);
    }

    /**
     * Configure the compression type. Default {@link CompressionType#SNAPPY}.
     *
     * @param compressionType compression type.
     * @throws IllegalArgumentException if {@code compressionType} is null
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setCompressionType(CompressionType compressionType) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.compressionType(compressionType);
    }

    /**
     * Configure whether to create the database if missing. Default true.
     *
     * @param createIfMissing true to create if missing
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setCreateIfMissing(boolean createIfMissing) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.createIfMissing(createIfMissing);
    }

    /**
     * Configure whether to throw an error if the database already exists. Default false.
     *
     * @param errorIfExists true for error if database exists
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setErrorIfExists(boolean errorIfExists) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.errorIfExists(errorIfExists);
    }

    /**
     * Configure the maximum number of open files. Default 1000.
     *
     * @param maxOpenFiles maximum number of open files
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setMaxOpenFiles(int maxOpenFiles) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.maxOpenFiles(maxOpenFiles);
    }

    /**
     * Configure whether paranoid checks are enabled. Default false.
     *
     * @param paranoidChecks true to enable
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setParanoidChecks(boolean paranoidChecks) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.paranoidChecks(paranoidChecks);
    }

    /**
     * Configure whether to verify checksums. Default true.
     *
     * @param verifyChecksums true to enable
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setVerifyChecksums(boolean verifyChecksums) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.verifyChecksums(verifyChecksums);
    }

    /**
     * Configure the write buffer size. Default 4MB.
     *
     * @param writeBufferSize write buffer size
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setWriteBufferSize(int writeBufferSize) {
        Preconditions.checkState(this.db == null, "already started");
        this.options = this.options.writeBufferSize(writeBufferSize);
    }

// Lifecycle

    @Override
    @PostConstruct
    public synchronized void start() {

        // Already started?
        if (this.db != null)
            return;
        this.log.info("starting {}", this);

        // Check configuration
        Preconditions.checkState(this.directory != null, "no directory configured");

        // Create directory if needed
        if (!this.directory.exists()) {
            if (!this.options.createIfMissing())
                throw new RuntimeException(String.format("directory \"%s\" does not exist", this.directory));
            if (!this.directory.mkdirs())
                throw new RuntimeException(String.format("failed to create directory \"%s\"", this.directory));
        }
        if (!this.directory.isDirectory())
            throw new RuntimeException(String.format("file \"%s\" is not a directory", this.directory));

        // Open database
        if (this.log.isDebugEnabled())
            this.log.debug("opening {} LevelDB database", this);
        try {
            this.db = this.factory.open(this.directory, this.options);
        } catch (IOException e) {
            throw new RuntimeException("LevelDB database startup failed", e);
        }

        // Create k/v store view
        this.kv = new LevelDBKVStore(this.db, new ReadOptions().verifyChecksums(this.options.verifyChecksums()), null);

        // Add shutdown hook so we don't leak native resources
        if (this.shutdownHookRegistered.compareAndSet(false, true))
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    @Override
    @PreDestroy
    public synchronized void stop() {

        // Check state
        if (this.db == null)
            return;
        this.log.info("stopping {}", this);

        // Close k/v store view
        this.kv.close();
        this.kv = null;

        // Shut down LevelDB database
        try {
            if (this.log.isDebugEnabled())
                this.log.info("closing {} LevelDB database", this);
            this.db.close();
        } catch (Throwable e) {
            this.log.error("caught exception closing database during shutdown (ignoring)", e);
        }
        this.db = null;
    }

// ForwardingKVStore

    @Override
    protected synchronized LevelDBKVStore delegate() {
        Preconditions.checkState(this.db != null && this.kv != null, "closed");
        return this.kv;
    }

// AtomicKVStore

    @Override
    public synchronized SnapshotLevelDBKVStore readOnlySnapshot() {
        Preconditions.checkState(this.db != null, "closed");
        return new SnapshotLevelDBKVStore(this.db, this.options.verifyChecksums());
    }

    @Override
    public synchronized void apply(Mutations mutations, boolean sync) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        Preconditions.checkState(this.db != null, "closed");

        // Apply mutations in a batch
        try (WriteBatch batch = this.db.createWriteBatch()) {

            // Apply removes
            final ReadOptions iteratorOptions = new ReadOptions().verifyChecksums(this.options.verifyChecksums()).fillCache(false);
            try (Stream<KeyRange> removes = mutations.getRemoveRanges()) {
                removes.iterator().forEachRemaining(range -> {
                    final ByteData min = range.getMin();
                    final ByteData max = range.getMax();
                    if (min != null && max != null && ByteUtil.isConsecutive(min, max))
                        batch.delete(min.toByteArray());
                    else {
                        final LevelDBKVStore.Iterator i;
                        synchronized (this) {
                            i = this.kv.createIterator(iteratorOptions, min, max, false);
                        }
                        try {
                            while (i.hasNext())
                                batch.delete(i.next().getKey().toByteArray());
                        } finally {
                            i.close();
                        }
                    }
                });
            }

            // Apply puts
            try (Stream<Map.Entry<ByteData, ByteData>> puts = mutations.getPutPairs()) {
                puts.iterator().forEachRemaining(entry -> batch.put(entry.getKey().toByteArray(), entry.getValue().toByteArray()));
            }

            // Convert counter adjustments into puts and apply them
            try (Stream<Map.Entry<ByteData, Long>> adjusts = mutations.getAdjustPairs()) {
                for (Iterator<Map.Entry<ByteData, Long>> i = adjusts.iterator(); i.hasNext(); ) {
                    final Map.Entry<ByteData, Long> adjust = i.next();

                    // Decode old value
                    final ByteData key = adjust.getKey();
                    final long diff = adjust.getValue();
                    ByteData oldBytes = this.kv.get(key);
                    if (oldBytes == null)
                        oldBytes = ByteData.zeros(8);
                    final long oldValue;
                    try {
                        oldValue = this.kv.decodeCounter(oldBytes);
                    } catch (IllegalArgumentException e) {
                        return;
                    }

                    // Add adjustment and put new value
                    batch.put(key.toByteArray(), this.kv.encodeCounter(oldValue + diff).toByteArray());
                }
            }

            // Write the batch
            this.db.write(batch, new WriteOptions().sync(sync));
        } catch (IOException e) {
            throw new DBException("error applying changes to LevelDB", e);
        }
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #stop} to close any unclosed iterators.
     */
    @Override
    @SuppressWarnings("deprecation")
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
