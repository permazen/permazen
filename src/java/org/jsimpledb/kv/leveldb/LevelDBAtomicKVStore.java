
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.leveldb;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.kv.util.ForwardingKVStore;
import org.jsimpledb.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AtomicKVStore} view of a LevelDB database.
 *
 * <p>
 * A {@linkplain #setDirectory database directory} is the only required configuration property.
 * Instances may be stopped and (re)started multiple times.
 */
public class LevelDBAtomicKVStore extends ForwardingKVStore implements AtomicKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final AtomicBoolean shutdownHookRegistered = new AtomicBoolean();
    private final DBFactory factory;

    private Options options = new Options().createIfMissing(true).logger(new org.iq80.leveldb.Logger() {
        @Override
        public void log(String message) {
            LevelDBAtomicKVStore.this.log.info("[LevelDB] " + message);
        }
      });
    private File directory;

    private LevelDBKVStore kv;
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
            this.log.debug("opening " + this + " LevelDB database");
        try {
            this.db = this.factory.open(this.directory, this.options);
        } catch (IOException e) {
            throw new RuntimeException("LevelDB database startup failed", e);
        }

        // Create k/v store view
        this.kv = new LevelDBKVStore(this.db, new ReadOptions().verifyChecksums(this.options.verifyChecksums()), null);

        // Add shutdown hook so we don't leak native resources
        if (this.shutdownHookRegistered.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    LevelDBAtomicKVStore.this.stop();
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

        // Shut down LevelDB database
        try {
            if (this.log.isDebugEnabled())
                this.log.info("closing " + this + " LevelDB database");
            this.db.close();
        } catch (Throwable e) {
            this.log.error("caught exception closing database during shutdown (ignoring)", e);
        }
        this.db = null;
    }

// ForwardingKVStore

    @Override
    protected LevelDBKVStore delegate() {
        Preconditions.checkState(this.db != null && this.kv != null, "closed");
        return this.kv;
    }

// AtomicKVStore

    @Override
    public synchronized SnapshotLevelDBKVStore snapshot() {
        Preconditions.checkState(this.db != null, "closed");
        return new SnapshotLevelDBKVStore(this.db, this.options.verifyChecksums());
    }

    @Override
    public synchronized void mutate(Mutations mutations, boolean sync) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        Preconditions.checkState(this.db != null, "closed");

        // Apply mutations in a batch
        try (WriteBatch batch = this.db.createWriteBatch()) {

            // Apply removes
            final ReadOptions iteratorOptions = new ReadOptions().verifyChecksums(this.options.verifyChecksums()).fillCache(false);
            for (KeyRange range : mutations.getRemoveRanges()) {
                final byte[] min = range.getMin();
                final byte[] max = range.getMax();
                if (min != null && max != null && ByteUtil.compare(max, ByteUtil.getNextKey(min)) == 0)
                    batch.delete(min);
                else {
                    try (LevelDBKVStore.Iterator i = this.kv.createIteator(iteratorOptions, min, max, false)) {
                        while (i.hasNext())
                            batch.delete(i.next().getKey());
                    }
                }
            }

            // Apply puts
            for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs())
                batch.put(entry.getKey(), entry.getValue());

            // Convert counter adjustments into puts
            final Function<Map.Entry<byte[], Long>, Map.Entry<byte[], byte[]>> counterPutFunction
              = new Function<Map.Entry<byte[], Long>, Map.Entry<byte[], byte[]>>() {
                @Override
                public Map.Entry<byte[], byte[]> apply(Map.Entry<byte[], Long> adjust) {

                    // Decode old value
                    final byte[] key = adjust.getKey();
                    final long diff = adjust.getValue();
                    byte[] oldBytes = LevelDBAtomicKVStore.this.kv.get(key);
                    if (oldBytes == null)
                        oldBytes  = new byte[8];
                    final long oldValue;
                    try {
                        oldValue = LevelDBAtomicKVStore.this.kv.decodeCounter(oldBytes);
                    } catch (IllegalArgumentException e) {
                        return null;
                    }

                    // Add adjustment and re-encode it
                    return new AbstractMap.SimpleEntry<byte[], byte[]>(key,
                      LevelDBAtomicKVStore.this.kv.encodeCounter(oldValue + diff));
                }
            };

            // Apply counter adjustments
            for (Map.Entry<byte[], byte[]> entry : Iterables.transform(mutations.getAdjustPairs(), counterPutFunction)) {
                if (entry != null)
                    batch.put(entry.getKey(), entry.getValue());
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

