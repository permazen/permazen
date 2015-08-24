
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.util.CloseableForwardingKVStore;
import org.jsimpledb.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AtomicKVStore} based on {@link ArrayKVStore} with background compaction.
 *
 * <p>
 * This implementation is designed to maximize the speed of reads and minimize the amount of memory overhead per key/value pair.
 * It is optimized for relatively infrequent writes.
 *
 * <p>
 * Instances periodically compact outstanding changes into new arrays in a background thread. A compaction is scheduled whenever:
 *  <ul>
 *  <li>The size of uncompacted changes exceeds the {@linkplain #setCompactLowWater compaction space low-water mark}</li>
 *  <li>The oldest uncompacted modification is older than the {@linkplain #setCompactMaxDelay compaction maximum delay}</li>
 *  <li>{@link #scheduleCompaction} is invoked</li>
 *  </ul>
 *
 * <p>
 * There is also a {@linkplain #setCompactHighWater high water mark for the size of uncompacted changes}: when this value
 * is exceeded, new write attempts will block until the current compaction cycle completes. This prevents the unbounded growth
 * of uncompacted changes when there is extremely high write volume.
 *
 * <p>
 * The {@linkplain #setDirectory database directory} is a required configuration property.
 *
 * <p>
 * Key and value data each must not exceed 2GB total.
 *
 * <p>
 * Instances may be stopped and (re)started multiple times.
 */
public class AtomicArrayKVStore extends AbstractKVStore implements AtomicKVStore {

    /**
     * Default compaction maximum delay in seconds ({@value #DEFAULT_COMPACTION_MAX_DELAY} seconds).
     */
    public static final int DEFAULT_COMPACTION_MAX_DELAY = 90;

    /**
     * Default compaction space low-water mark in bytes ({@value #DEFAULT_COMPACTION_LOW_WATER} bytes).
     */
    public static final int DEFAULT_COMPACTION_LOW_WATER = 64 * 1024;

    /**
     * Default compaction space high-water mark in bytes ({@value #DEFAULT_COMPACTION_HIGH_WATER} bytes).
     */
    public static final int DEFAULT_COMPACTION_HIGH_WATER = 1024 * 1024 * 1024;

    private static final int MIN_MMAP_LENGTH = 1024 * 1024;

    // Flags used by compact() merge
    private static final int MERGE_KV = 0x01;
    private static final int MERGE_PUT = 0x02;
    private static final int MERGE_ADJUST = 0x04;

    private static final String GENERATION_FILE_NAME = "gen";
    private static final String LOCK_FILE_NAME = "lockfile";
    private static final String INDX_FILE_NAME_BASE = "indx.";
    private static final String KEYS_FILE_NAME_BASE = "keys.";
    private static final String VALS_FILE_NAME_BASE = "vals.";
    private static final String MODS_FILE_NAME_BASE = "mods.";

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final ReentrantReadWriteLock.ReadLock readLock = this.lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = this.lock.writeLock();
    private final Condition compactionFinishedCondition = this.writeLock.newCondition();

    // Configuration state
    private File directory;
    private ExecutorService executorService;
    private int compactMaxDelay = DEFAULT_COMPACTION_MAX_DELAY;
    private int compactLowWater = DEFAULT_COMPACTION_LOW_WATER;
    private int compactHighWater = DEFAULT_COMPACTION_HIGH_WATER;

    // Runtime state
    private long generation;
    private boolean createdExecutorService;
    private File generationFile;
    private File lockFile;
    private FileChannel lockFileChannel;
    private File indxFile;
    private File keysFile;
    private File valsFile;
    private File modsFile;
    private FileOutputStream modsFileOutput;
    private FileChannel directoryChannel;
    private long modsFileLength;
    private long modsFileSyncPoint;
    private ByteBuffer indx;
    private ByteBuffer keys;
    private ByteBuffer vals;
    private ArrayKVStore kvstore;
    private MutableView mods;
    private Future<?> compactFuture;
    private long firstModTimestamp;

// Accessors

    /**
     * Get the filesystem directory containing the database. If not set, this class functions as an im-memory store.
     *
     * @return database directory, or null for none
     */
    public synchronized File getDirectory() {
        return this.directory;
    }

    /**
     * Configure the filesystem directory containing the database. If not set, this class functions as an im-memory store.
     *
     * @param directory database directory, or null for none
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setDirectory(File directory) {
        this.writeLock.lock();
        try {
            Preconditions.checkState(this.kvstore == null, "already started");
            this.directory = directory;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Configure the {@link ExecutorService} used when performing background compaction.
     *
     * <p>
     * If not explicitly configured, an {@link ExecutorService} will be created automatically during {@link #start}
     * using {@link Executors#newSingleThreadExecutor} and shutdown by {@link #stop}. If explicitly configured here,
     * the {@link ExecutorService} will not be shutdown by {@link #stop}.
     *
     * @param executorService executor service
     * @throws IllegalStateException if this instance is already {@link #start}ed
     * @throws IllegalArgumentException if {@code executorService} is null
     */
    public void setExecutorService(ExecutorService executorService) {
        this.writeLock.lock();
        try {
            Preconditions.checkState(this.kvstore == null, "already started");
            this.executorService = executorService;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Configure the compaction time maximum delay. Compaction will be automatically triggered whenever there is any
     * uncompacted modification older than this.
     *
     * @param compactMaxDelay compaction time maximum delay in seconds
     * @throws IllegalArgumentException if {@code compactMaxDelay} is negative
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setCompactMaxDelay(int compactMaxDelay) {
        Preconditions.checkState(compactMaxDelay >= 0, "negative value");
        this.writeLock.lock();
        try {
            Preconditions.checkState(this.kvstore == null, "already started");
            this.compactMaxDelay = compactMaxDelay;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Configure the compaction space low-water mark in bytes.
     *
     * @param compactLowWater compaction space low-water mark in bytes
     * @throws IllegalArgumentException if {@code compactLowWater} is negative
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setCompactLowWater(int compactLowWater) {
        Preconditions.checkState(compactLowWater >= 0, "negative value");
        this.writeLock.lock();
        try {
            Preconditions.checkState(this.kvstore == null, "already started");
            this.compactLowWater = compactLowWater;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Configure the compaction space high-water mark in bytes.
     *
     * <p>
     * If the compaction space high water mark is set smaller than the the compaction space low water mark,
     * then it's treated as if it were the same as the compaction space low water mark.
     *
     * @param compactHighWater compaction space high-water mark in bytes
     * @throws IllegalArgumentException if {@code compactHighWater} is negative
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setCompactHighWater(int compactHighWater) {
        Preconditions.checkState(compactHighWater >= 0, "negative value");
        this.writeLock.lock();
        try {
            Preconditions.checkState(this.kvstore == null, "already started");
            this.compactHighWater = compactHighWater;
        } finally {
            this.writeLock.unlock();
        }
    }

// Lifecycle

    @Override
    @PostConstruct
    public void start() {
        boolean success = false;
        this.writeLock.lock();
        try {

            // Already started?
            if (this.kvstore != null) {
                success = true;
                return;
            }
            this.log.info("starting " + this);

            // Sanity check
            assert this.compactFuture == null;
            assert this.executorService == null;
            assert !this.createdExecutorService;
            assert this.generation == 0;
            assert this.generationFile == null;
            assert this.lockFile == null;
            assert this.lockFileChannel == null;
            assert this.indxFile == null;
            assert this.keysFile == null;
            assert this.valsFile == null;
            assert this.modsFile == null;
            assert this.modsFileOutput == null;
            assert this.directoryChannel == null;
            assert this.modsFileLength == 0;
            assert this.modsFileSyncPoint == 0;
            assert this.indx == null;
            assert this.keys == null;
            assert this.vals == null;
            assert this.kvstore == null;
            assert this.mods == null;
            assert this.firstModTimestamp == 0;

            // Check configuration
            Preconditions.checkState(this.directory != null, "no directory configured");

            // Create executor if needed
            this.createdExecutorService = this.executorService == null;
            if (this.createdExecutorService) {
                this.executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable action) {
                        final Thread thread = new Thread(action);
                        thread.setName("Compactor for " + AtomicArrayKVStore.this);
                        return thread;
                    }
                });
            }

            // Create directory if needed
            boolean initializeFiles = false;
            if (!this.directory.exists()) {
                if (!this.directory.mkdirs())
                    throw new ArrayKVException("failed to create directory `" + this.directory + "'");
            }
            if (!this.directory.isDirectory())
                throw new ArrayKVException("file `" + this.directory + "' is not a directory");

            // Get directory channel we can fsync()
            this.directoryChannel = FileChannel.open(this.directory.toPath());

            // Open and lock the lock file
            this.lockFile = new File(this.directory, LOCK_FILE_NAME);
            if (!this.lockFile.exists()) {
                this.lockFileChannel = FileChannel.open(this.lockFile.toPath(),
                  StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            } else {
                this.lockFileChannel = FileChannel.open(this.lockFile.toPath(),
                  StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            }
            FileLock fileLock = null;
            try {
                fileLock = this.lockFileChannel.tryLock();
            } catch (OverlappingFileLockException e) {
                // too bad
            }
            if (fileLock == null)
                throw new ArrayKVException("database is already locked by another process or thread");

            // If no generation file exists, initialize generation zero
            this.generationFile = new File(this.directory, GENERATION_FILE_NAME);
            if (!this.generationFile.exists()) {

                // Create empty index, keys, and values files
                try (
                  final FileOutputStream indxOutput = new FileOutputStream(new File(this.directory, INDX_FILE_NAME_BASE + 0));
                  final FileOutputStream keysOutput = new FileOutputStream(new File(this.directory, KEYS_FILE_NAME_BASE + 0));
                  final FileOutputStream valsOutput = new FileOutputStream(new File(this.directory, VALS_FILE_NAME_BASE + 0));
                  final ArrayKVWriter arrayWriter = new ArrayKVWriter(indxOutput, keysOutput, valsOutput)) {
                    valsOutput.getChannel().force(false);
                    keysOutput.getChannel().force(false);
                    indxOutput.getChannel().force(false);
                    arrayWriter.flush();                                                        // avoid compiler warning
                }

                // Create generation file
                try (FileOutputStream output = new FileOutputStream(this.generationFile)) {
                    new PrintStream(output, true).println(0);
                    output.getChannel().force(false);
                }
            }

            // Read current generation number
            try {
                this.generation = Long.parseLong(
                  new String(Files.readAllBytes(this.generationFile.toPath()), StandardCharsets.UTF_8).trim(), 10);
                if (this.generation < 0)
                    throw new ArrayKVException("read negative generation number from " + this.generationFile);
            } catch (Exception e) {
                throw new ArrayKVException("error reading generation file", e);
            }

            // Set corresponding filenames
            this.indxFile = new File(this.directory, INDX_FILE_NAME_BASE + this.generation);
            this.keysFile = new File(this.directory, KEYS_FILE_NAME_BASE + this.generation);
            this.valsFile = new File(this.directory, VALS_FILE_NAME_BASE + this.generation);
            this.modsFile = new File(this.directory, MODS_FILE_NAME_BASE + this.generation);

            // Scan directory for unexpected files
            final List<File> expectedFiles = Arrays.asList(this.lockFile, this.generationFile,
              this.indxFile, this.keysFile, this.valsFile, this.modsFile);
            for (File file : this.directory.listFiles()) {
                if (!expectedFiles.contains(file))
                    this.log.warn("ignoring unexpected file " + file.getName() + " in my database directory");
            }

            // Create buffers that wrap the index, keys, and values files
            try (FileInputStream input = new FileInputStream(this.indxFile)) {
                this.indx = AtomicArrayKVStore.getBuffer(this.indxFile, input.getChannel());
            }
            try (FileInputStream input = new FileInputStream(this.keysFile)) {
                this.keys = AtomicArrayKVStore.getBuffer(this.keysFile, input.getChannel());
            }
            try (FileInputStream input = new FileInputStream(this.valsFile)) {
                this.vals = AtomicArrayKVStore.getBuffer(this.valsFile, input.getChannel());
            }

            // Set up underlying k/v store and uncompacted modifications
            this.kvstore = new ArrayKVStore(this.indx, this.keys, this.vals);
            this.mods = new MutableView(this.kvstore, null, new Writes());

            // Setup modifications file
            this.modsFileOutput = new FileOutputStream(this.modsFile, true);
            this.modsFileLength = this.modsFileOutput.getChannel().size();
            this.modsFileSyncPoint = this.modsFileLength;

            // Read and apply pre-existing uncompacted modifications from modifications file
            if (this.modsFileLength > 0) {
                this.log.info("reading " + this.modsFileLength + " bytes of uncompacted modifications from " + this.modsFile);
                try (FileInputStream input = new FileInputStream(this.modsFile)) {
                    while (input.available() > 0) {
                        final Writes writes;
                        try {
                            writes = Writes.deserialize(input);
                        } catch (Exception e) {
                            break;                                                      // probably a partial write
                        }
                        writes.applyTo(this.mods);
                    }
                    this.firstModTimestamp = System.nanoTime() | 1;                     // avoid zero value which is special
                }
            }

            // Schedule compaction if necessary
            if (this.isCompactionNeeded())
                this.scheduleCompaction();

            // Done
            success = true;
        } catch (IOException e) {
            throw new ArrayKVException("startup failed", e);
        } finally {
            try {
                if (!success)
                    this.cleanup();
            } finally {
                this.writeLock.unlock();
            }
        }
    }

    @Override
    @PreDestroy
    public void stop() {
        this.writeLock.lock();
        try {

            // Check state
            if (this.kvstore == null)
                return;
            this.log.info("stopping " + this);

            // Cleanup
            this.cleanup();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void cleanup() {

        // Should hold write lock now
        assert this.lock.isWriteLockedByCurrentThread();

        // Wait for compaction to complete (if any)
        while (this.compactFuture != null) {
            try {
                this.compactionFinishedCondition.await();
            } catch (InterruptedException e) {
                throw new ArrayKVException("thread was interrupted while waiting for compaction to complete", e);
            }
        }

        // Shut down executor - only if we created it
        if (this.createdExecutorService) {
            this.executorService.shutdownNow();
            this.executorService = null;
            this.createdExecutorService = false;
        }

        // Close files
        for (Closeable closeable : new Closeable[] { this.modsFileOutput, this.directoryChannel, this.lockFileChannel }) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }

        // Reset state
        this.generation = 0;
        this.generationFile = null;
        this.lockFile = null;
        this.lockFileChannel = null;
        this.indxFile = null;
        this.keysFile = null;
        this.valsFile = null;
        this.modsFile = null;
        this.modsFileOutput = null;
        this.directoryChannel = null;
        this.modsFileLength = 0;
        this.modsFileSyncPoint = 0;
        this.indx = null;
        this.keys = null;
        this.vals = null;
        this.kvstore = null;
        this.mods = null;
        this.firstModTimestamp = 0;
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.get(key);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.getAtLeast(minKey);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.getAtMost(maxKey);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.getRange(minKey, maxKey, reverse);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        final Writes writes = new Writes();
        writes.getPuts().put(key, value);
        this.mutate(writes, false);
    }

    @Override
    public void remove(byte[] key) {
        final Writes writes = new Writes();
        writes.setRemoves(new KeyRanges(key));
        this.mutate(writes, false);
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        final Writes writes = new Writes();
        writes.setRemoves(new KeyRanges(minKey != null ? minKey : ByteUtil.EMPTY, maxKey));
        this.mutate(writes, false);
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        final Writes writes = new Writes();
        writes.getAdjusts().put(key, amount);
        this.mutate(writes, false);
    }

    @Override
    public byte[] encodeCounter(long value) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.encodeCounter(value);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.decodeCounter(bytes);
        } finally {
            this.readLock.unlock();
        }
    }

// AtomicKVStore

    @Override
    public CloseableKVStore snapshot() {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");

            // Get oustanding modifications; if we are compacting, there are two sets of modifications
            final Writes writes1;
            if (this.mods.getKVStore() instanceof MutableView) {
                final MutableView oldMods = (MutableView)this.mods.getKVStore();
                assert oldMods.getKVStore() == this.kvstore;
                writes1 = oldMods.getWrites();
            } else
                writes1 = null;
            final Writes writes2 = this.mods.getWrites();

            // Clone (non-empty) writes to construct snapshot
            KVStore snapshot = this.kvstore;
            if (writes1 != null && !writes1.isEmpty())
                snapshot = new MutableView(snapshot, null, writes1.clone());
            if (writes2 != null && !writes2.isEmpty())
                snapshot = new MutableView(snapshot, null, writes2.clone());

            // Done
            return new CloseableForwardingKVStore(snapshot);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void mutate(Mutations mutations, final boolean sync) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        this.writeLock.lock();
        try {

            // Verify we are started
            Preconditions.checkState(this.kvstore != null, "not started");

            // Block until compaction completes if size of outstanding modifications exceeds high water mark
            while (this.compactFuture != null && this.modsFileLength > this.compactHighWater) {
                try {
                    this.compactionFinishedCondition.await();
                } catch (InterruptedException e) {
                    throw new ArrayKVException("thread was interrupted while waiting for compaction to complete", e);
                }
            }

            // Append mutations to uncompacted mods file
            final Writes writes;
            if (mutations instanceof Writes)
                writes = (Writes)mutations;
            else {
                writes = new Writes();
                writes.setRemoves(new KeyRanges(mutations.getRemoveRanges()));
                for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs())
                    writes.getPuts().put(entry.getKey(), entry.getValue());
                for (Map.Entry<byte[], Long> entry : mutations.getAdjustPairs())
                    writes.getAdjusts().put(entry.getKey(), entry.getValue());
            }
            try {
                writes.serialize(this.modsFileOutput);
            } catch (IOException e) {
                try {
                    this.modsFileOutput.getChannel().truncate(this.modsFileLength);              // undo append
                } catch (IOException e2) {
                    this.log.error("error truncating log file (ignoring)", e2);
                }
                throw new ArrayKVException("error appending to " + this.modsFile, e);
            }
            final long newModsFileLength;
            try {
                newModsFileLength = this.modsFileOutput.getChannel().size();
            } catch (IOException e) {
                throw new ArrayKVException("error getting length of " + this.modsFile, e);
            }
            if (this.log.isDebugEnabled()) {
                this.log.debug("appended " + (newModsFileLength - this.modsFileLength) + " bytes to "
                  + this.modsFile + " (new length " + newModsFileLength + ")");
            }
            this.modsFileLength = newModsFileLength;

            // Apply removes
            for (KeyRange range : mutations.getRemoveRanges()) {
                final byte[] min = range.getMin();
                final byte[] max = range.getMax();
                this.mods.removeRange(min, max);
            }

            // Apply puts
            for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs()) {
                final byte[] key = entry.getKey();
                final byte[] val = entry.getValue();
                this.mods.put(key, val);
            }

            // Apply counter adjustments
            for (Map.Entry<byte[], Long> entry : mutations.getAdjustPairs()) {
                final byte[] key = entry.getKey();
                final long adjust = entry.getValue();
                this.mods.adjustCounter(key, adjust);
            }

            // Set first uncompacted modification timestamp, if not already set
            if (this.firstModTimestamp == 0)
                this.firstModTimestamp = System.nanoTime() | 1;                 // avoid zero value which is special

            // Trigger compaction if necessary
            if (this.isCompactionNeeded())
                this.scheduleCompaction();

            // If we're not syncing, we're don
            if (!sync)
                return;

            // Update sync point, so compaction knows to also sync these mods when it copies them
            this.modsFileSyncPoint = this.modsFileLength;

            // Downgrade lock
            this.readLock.lock();
        } finally {
            this.writeLock.unlock();
        }

        // Sync the mods file while holding only the read lock
        try {
            this.modsFileOutput.getChannel().force(false);
        } catch (IOException e) {
            this.log.error("error syncing log file (ignoring)", e);
        } finally {
            this.readLock.unlock();
        }
    }

// Compaction

    /**
     * Schedule a new compaction cycle, unless there is one already scheduled or running, or there are no
     * outstanding uncompacted modifications.
     *
     * @return a future for the completion of the next compaction cycle, or null if there are no uncompacted modifications
     * @throws IllegalStateException if this instance is not started
     */
    public Future<?> scheduleCompaction() {
        this.writeLock.lock();
        try {

            // Sanity check
            Preconditions.checkState(this.kvstore != null, "not started");
            if (this.modsFileLength == 0)
                return null;

            // Schedule new compaction unless already scheduled/running
            if (this.compactFuture == null) {
                this.compactFuture = this.executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            AtomicArrayKVStore.this.compact();
                        } catch (Throwable t) {
                            AtomicArrayKVStore.this.log.error("error during compaction", t);
                        }
                    }
                });
            }

            // Done
            return this.compactFuture;
        } finally {
            this.writeLock.unlock();
        }
    }

    private boolean isCompactionNeeded() {

        // Should hold write lock now
        assert this.lock.isWriteLockedByCurrentThread();

        // Already scheduled?
        if (this.compactFuture != null)
            return false;

        // Is there anything to do?
        if (this.modsFileLength == 0)
            return false;

        // Check space low-water
        if (this.modsFileLength > this.compactLowWater)
            return true;

        // Check maximum compaction delay
        if (this.firstModTimestamp != 0) {
            final long firstModAge = (System.nanoTime() - this.firstModTimestamp) / 1000000000L;   // convert ns -> sec
            if (firstModAge >= this.compactMaxDelay)
                return true;
        }

        // Not yet
        return false;
    }

    @SuppressWarnings("fallthrough")
    private void compact() throws IOException {
        final long compactionStartTime;
        try {

            // Snapshot (and wrap) pending modifications, and mark log file position
            final Writes writesToCompact;
            final long previousModsFileLength;
            final long previousModsFileSyncPoint;
            this.writeLock.lock();
            try {

                // Mark start time and get uncompacted modifications
                assert this.modsFileLength > 0;
                compactionStartTime = System.nanoTime();
                writesToCompact = this.mods.getWrites();

                // It's possible the uncompacted modifications are a no-op; if so, no compaction is necessary
                if (writesToCompact.isEmpty()) {
                    this.modsFileOutput.getChannel().truncate(0);
                    this.modsFileLength = 0;
                    this.modsFileSyncPoint = 0;
                    this.modsFileOutput.getChannel().force(false);
                    return;
                }

                // Allow new modifications to be added by other threads while we are compacting the old modifications
                this.mods = new MutableView(this.mods, null, new Writes());
                previousModsFileLength = this.modsFileLength;
                previousModsFileSyncPoint = this.modsFileSyncPoint;
            } finally {
                this.writeLock.unlock();
            }
            if (this.log.isDebugEnabled()) {
                this.log.debug("starting compaction for generation " + this.generation + " -> " + (this.generation + 1)
                  + " with mods file length " + previousModsFileLength);
            }

            // Create the next generation
            final long newGeneration = this.generation + 1;
            final File newIndxFile = new File(this.directory, INDX_FILE_NAME_BASE + newGeneration);
            final File newKeysFile = new File(this.directory, KEYS_FILE_NAME_BASE + newGeneration);
            final File newValsFile = new File(this.directory, VALS_FILE_NAME_BASE + newGeneration);
            final File newModsFile = new File(this.directory, MODS_FILE_NAME_BASE + newGeneration);
            ByteBuffer newIndx = null;
            ByteBuffer newKeys = null;
            ByteBuffer newVals = null;
            FileOutputStream newModsFileOutput = null;
            boolean success = false;
            try {

                // Merge existing compacted key/value data with uncompacted modifications
                try (
                  final FileOutputStream indxOutput = new FileOutputStream(newIndxFile);
                  final FileOutputStream keysOutput = new FileOutputStream(newKeysFile);
                  final FileOutputStream valsOutput = new FileOutputStream(newValsFile);
                  final ArrayKVWriter arrayWriter = new ArrayKVWriter(indxOutput, keysOutput, valsOutput)) {

                    // Initialize iterators
                    final Iterator<KVPair> kvIterator = this.kvstore.getRange(null, null, false);
                    final Iterator<KeyRange> removeIterator = writesToCompact.getRemoves().iterator();
                    final Iterator<Map.Entry<byte[], byte[]>> putIterator = writesToCompact.getPuts().entrySet().iterator();
                    final Iterator<Map.Entry<byte[], Long>> adjustIterator = writesToCompact.getAdjusts().entrySet().iterator();

                    // Merge iterators and write the merged result to the ArrayKVWriter
                    KVPair kv = kvIterator.hasNext() ? kvIterator.next() : null;
                    KeyRange remove = removeIterator.hasNext() ? removeIterator.next() : null;
                    Map.Entry<byte[], byte[]> put = putIterator.hasNext() ? putIterator.next() : null;
                    Map.Entry<byte[], Long> adjust = adjustIterator.hasNext() ? adjustIterator.next() : null;
                    while (true) {

                        // Find the minimum key among remaining (a) key/value pairs, (b) puts, and (c) adjusts
                        byte[] key = null;
                        if (kv != null)
                            key = kv.getKey();
                        if (put != null && (key == null || ByteUtil.compare(put.getKey(), key) < 0))
                            key = put.getKey();
                        if (adjust != null && (key == null || ByteUtil.compare(adjust.getKey(), key) < 0))
                            key = adjust.getKey();
                        if (key == null)                                                        // we're done
                            break;

                        // Determine which inputs apply to the current key
                        int inputs = 0;
                        if (kv != null && Arrays.equals(kv.getKey(), key))
                            inputs |= MERGE_KV;
                        if (put != null && Arrays.equals(put.getKey(), key))
                            inputs |= MERGE_PUT;
                        if (adjust != null && Arrays.equals(adjust.getKey(), key))
                            inputs |= MERGE_ADJUST;

                        // Find the removal that applies to this next key, if any
                        while (remove != null) {
                            final byte[] removeMax = remove.getMax();
                            if (removeMax != null && ByteUtil.compare(removeMax, key) <= 0) {
                                remove = removeIterator.hasNext() ? removeIterator.next() : null;
                                continue;
                            }
                            break;
                        }
                        final boolean removed = remove != null && ByteUtil.compare(remove.getMin(), key) <= 0;

                        // Merge the applicable inputs
                        switch (inputs) {
                        case 0:                                                     // k/v store entry was removed
                            break;
                        case MERGE_KV:
                            if (!removed)
                                arrayWriter.writeKV(kv.getKey(), kv.getValue());
                            break;
                        case MERGE_PUT:
                        case MERGE_PUT | MERGE_KV:
                            arrayWriter.writeKV(put.getKey(), put.getValue());
                            break;
                        case MERGE_ADJUST:                                          // adjusted a non-existent value; ignore
                            break;
                        case MERGE_ADJUST | MERGE_KV:
                            if (removed)                                            // adjusted a removed value; ignore
                                break;
                            // FALLTHROUGH
                        case MERGE_ADJUST | MERGE_PUT:
                        case MERGE_ADJUST | MERGE_PUT | MERGE_KV:
                        {
                            final byte[] encodedCount = (inputs & MERGE_PUT) != 0 ? put.getValue() : kv.getValue();
                            final long counter;
                            try {
                                counter = this.kvstore.decodeCounter(encodedCount);
                            } catch (IllegalArgumentException e) {
                                break;
                            }
                            final byte[] value = this.kvstore.encodeCounter(counter + adjust.getValue());
                            arrayWriter.writeKV(key, value);
                            break;
                        }
                        default:
                            throw new RuntimeException();
                        }

                        // Advance k/v, put, and/or adjust
                        if ((inputs & MERGE_KV) != 0)
                            kv = kvIterator.hasNext() ? kvIterator.next() : null;
                        if ((inputs & MERGE_PUT) != 0)
                            put = putIterator.hasNext() ? putIterator.next() : null;
                        if ((inputs & MERGE_ADJUST) != 0)
                            adjust = adjustIterator.hasNext() ? adjustIterator.next() : null;
                    }

                    // Sync file data
                    arrayWriter.flush();
                    valsOutput.getChannel().force(false);
                    keysOutput.getChannel().force(false);
                    indxOutput.getChannel().force(false);
                }
                assert newIndxFile.exists();
                assert newKeysFile.exists();
                assert newValsFile.exists();

                // Create byte buffers from new files
                try (FileInputStream input = new FileInputStream(newIndxFile)) {
                    newIndx = AtomicArrayKVStore.getBuffer(newIndxFile, input.getChannel());
                }
                try (FileInputStream input = new FileInputStream(newKeysFile)) {
                    newKeys = AtomicArrayKVStore.getBuffer(newKeysFile, input.getChannel());
                }
                try (FileInputStream input = new FileInputStream(newValsFile)) {
                    newVals = AtomicArrayKVStore.getBuffer(newValsFile, input.getChannel());
                }

                // Create new, empty mods file
                newModsFile.delete();                                           // shouldn't exist, but just in case...
                newModsFileOutput = new FileOutputStream(newModsFile, true);
                assert newModsFile.exists();

                // Sync directory
                this.directoryChannel.force(false);

                // We're done creating files
                success = true;
            } finally {

                // Finish up
                this.writeLock.lock();
                try {
                    if (success) {

                        // Initialize for wrap-up
                        success = false;

                        // Size up additional mods
                        final long additionalModsLength = this.modsFileLength - previousModsFileLength;

                        // Logit
                        if (this.log.isDebugEnabled()) {
                            final float duration = (System.nanoTime() - compactionStartTime) / 1000000000f;
                            this.log.debug("compaction for generation " + this.generation + " -> " + (this.generation + 1)
                              + " finishing up with " + additionalModsLength + " bytes of new modifications after "
                              + String.format("%.4s", duration) + " seconds");
                        }

                        // Apply any changes that were made while we were unlocked and writing files
                        long newModsFileLength = 0;
                        long newModsFileSyncPoint = 0;
                        if (additionalModsLength > 0) {
                            try (
                              final FileChannel modsFileChannel = FileChannel.open(this.modsFile.toPath());
                              final FileChannel newModsFileChannel = FileChannel.open(newModsFile.toPath(),
                               StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {

                                // Append new data in old mods file to new mods file
                                while (newModsFileLength < additionalModsLength) {
                                    final long transferred = modsFileChannel.transferTo(
                                      previousModsFileLength + newModsFileLength, additionalModsLength - newModsFileLength,
                                      newModsFileChannel);
                                    assert transferred > 0;
                                    newModsFileLength += transferred;
                                }

                                // If any of that new data was fsync()'d, we must fsync() the copy of it in the new mods file
                                if (this.modsFileSyncPoint > previousModsFileSyncPoint) {
                                    newModsFileChannel.force(false);
                                    newModsFileSyncPoint = newModsFileLength;
                                }
                            }
                        }

                        // Atomically update new generation file contents
                        final AtomicUpdateFileOutputStream genOutput = new AtomicUpdateFileOutputStream(this.generationFile);
                        boolean genSuccess = false;
                        try {
                            new PrintStream(genOutput, true).println(newGeneration);
                            genOutput.getChannel().force(false);
                            genSuccess = true;
                        } finally {
                            if (genSuccess)
                                genOutput.close();
                            else
                                genOutput.cancel();
                        }

                        // Declare success
                        success = true;

                        // Remember old info so we can clean it up
                        final File oldIndxFile = this.indxFile;
                        final File oldKeysFile = this.keysFile;
                        final File oldValsFile = this.valsFile;
                        final File oldModsFile = this.modsFile;
                        final FileOutputStream oldModsFileOutput = this.modsFileOutput;

                        // Change to the new generation
                        this.generation = newGeneration;
                        this.indx = newIndx;
                        this.keys = newKeys;
                        this.vals = newVals;
                        this.indxFile = newIndxFile;
                        this.keysFile = newKeysFile;
                        this.valsFile = newValsFile;
                        this.modsFile = newModsFile;
                        this.modsFileOutput = newModsFileOutput;
                        this.modsFileLength = newModsFileLength;
                        this.modsFileSyncPoint = newModsFileSyncPoint;
                        this.kvstore = new ArrayKVStore(this.indx, this.keys, this.vals);
                        this.mods = new MutableView(this.kvstore, null, this.mods.getWrites());

                        // Sync directory prior to deleting files
                        try {
                            this.directoryChannel.force(false);
                        } catch (IOException e) {
                            this.log.error("error syncing directory " + this.directory + " (ignoring)", e);
                        }

                        // Delete old files
                        oldIndxFile.delete();
                        oldKeysFile.delete();
                        oldValsFile.delete();
                        oldModsFile.delete();

                        // Close old mods file output stream
                        try {
                            oldModsFileOutput.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                } finally {
                    try {

                        // Logit
                        if (this.log.isDebugEnabled()) {
                            final float duration = (System.nanoTime() - compactionStartTime) / 1000000000f;
                            this.log.debug("compaction for generation " + (newGeneration - 1) + " -> " + newGeneration
                              + (success ? " succeeded" : " failed") + " in " + String.format("%.4s", duration) + " seconds");
                        }

                        // Cleanup/undo on failure
                        if (!success) {

                            // Put back the old uncompacted modifications, and merge any new mods into them
                            final Writes writesDuringCompaction = this.mods.getWrites();
                            this.mods = new MutableView(this.kvstore, null, writesToCompact);
                            writesDuringCompaction.applyTo(this.mods);

                            // Delete the files we were creating
                            newIndxFile.delete();
                            newKeysFile.delete();
                            newValsFile.delete();
                            if (newModsFileOutput != null) {
                                newModsFile.delete();
                                try {
                                    newModsFileOutput.close();
                                } catch (Exception e) {
                                    // ignore
                                }
                            }
                        }
                    } finally {
                        this.writeLock.unlock();
                    }
                }
            }
        } finally {
            this.compactFuture = null;
            this.writeLock.lock();
            try {
                this.compactionFinishedCondition.signalAll();
            } finally {
                this.writeLock.unlock();
            }
        }
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #stop} to close any unclosed iterators.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            if (this.kvstore != null)
               this.log.warn(this + " leaked without invoking stop()");
            this.stop();
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.directory + "]";
    }

    private static ByteBuffer getBuffer(File file, FileChannel fileChannel) throws IOException {
        final long length = fileChannel.size();
        return length >= MIN_MMAP_LENGTH ?
          fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, length) :
          ByteBuffer.wrap(Files.readAllBytes(file.toPath())).asReadOnlyBuffer();
    }
}

