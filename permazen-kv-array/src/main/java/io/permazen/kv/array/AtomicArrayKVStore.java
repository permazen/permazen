
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ForwardingFuture;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.mvcc.Writes;
import io.permazen.kv.util.CloseableForwardingKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AtomicKVStore} based on {@link ArrayKVStore} plus a write-ahead log and background compaction.
 *
 * <p>
 * This implementation is designed to maximize the speed of reads and minimize the amount of memory overhead per key/value pair.
 * It is optimized for relatively infrequent writes.
 *
 * <p>
 * A (read-only) {@link ArrayKVStore} is the basis for the database; the array files are mapped into memory. As mutations are
 * applied, they are added to an in-memory change set, and appended to a mutation log file for persistence. On restart,
 * the mutation log file (if any) is read to reconstruct the in-memory change set.
 *
 * <p>
 * <b>Compaction</b>
 *
 * <p>
 * Instances periodically compact outstanding changes into new array files (and truncate the mutation log file) in a background
 * thread. A compaction is scheduled whenever:
 *  <ul>
 *  <li>The size of mutation log file exceeds the {@linkplain #setCompactLowWater compaction space low-water mark}</li>
 *  <li>The oldest uncompacted modification is older than the {@linkplain #setCompactMaxDelay compaction maximum delay}</li>
 *  <li>The {@link #scheduleCompaction} method is invoked</li>
 *  </ul>
 *
 * <p>
 * In order to prevent compaction from getting hopelessly behind when there is high write volume, a
 * {@linkplain #setCompactHighWater compaction space high-water mark} is also used. When the size of the mutation log file
 * exceeds the half-way point between the low-water and high-water marks, new write attempts start being artificially delayed,
 * and this delay amount {@linkplain #calculateCompactionPressureDelay increases to infinity} as the high-water mark is approached,
 * so that as long as the high-water mark is exceeded, new writes are blocked completely until the current compaction cycle
 * completes. Use {@link #getTotalMillisWaiting} to asses the total amount of time spent in these artificial delays.
 *
 * <p>
 * The number of bytes occupied by the in-memory change set and the length of the outstanding mutations log file are
 * related. Therefore, the compaction high-water mark loosely correlates to a maximum amount of memory required
 * by the in-memory change set.
 *
 * <p>
 * <b>Hot Backups</b>
 *
 * <p>
 * "Hot" backups may be created in parallel with normal operation via {@link #hotCopy hotCopy()}.
 * Hard links are used to make this operation fast; only the mutation log file (if any) is actually copied.
 *
 * <p>
 * The {@linkplain #setDirectory database directory} is a required configuration property.
 *
 * <p>
 * Key and value data must not exceed 2GB (each separately).
 *
 * <p>
 * Instances may be stopped and (re)started multiple times.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Write-ahead_logging">Write-ahead logging</a>
 */
@ThreadSafe
public class AtomicArrayKVStore extends AbstractKVStore implements AtomicKVStore {

    /**
     * Default compaction maximum delay in seconds ({@value #DEFAULT_COMPACTION_MAX_DELAY} seconds).
     */
    public static final int DEFAULT_COMPACTION_MAX_DELAY = 24 * 60 * 60;

    /**
     * Default compaction space low-water mark in bytes ({@value #DEFAULT_COMPACTION_LOW_WATER} bytes).
     */
    public static final int DEFAULT_COMPACTION_LOW_WATER = 64 * 1024;

    /**
     * Default compaction space high-water mark in bytes ({@value #DEFAULT_COMPACTION_HIGH_WATER} bytes).
     */
    public static final int DEFAULT_COMPACTION_HIGH_WATER = 1024 * 1024 * 1024;

    private static final int MIN_MMAP_LENGTH = 1024 * 1024;

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
    private final Condition hotCopyFinishedCondition = this.writeLock.newCondition();
    private final boolean suckyOS = this.isWindows();

    // Configuration state
    @GuardedBy("lock")
    private File directory;
    @GuardedBy("lock")
    private ScheduledExecutorService scheduledExecutorService;
    @GuardedBy("lock")
    private int compactMaxDelay = DEFAULT_COMPACTION_MAX_DELAY;
    @GuardedBy("lock")
    private int compactLowWater = DEFAULT_COMPACTION_LOW_WATER;
    @GuardedBy("lock")
    private int compactHighWater = DEFAULT_COMPACTION_HIGH_WATER;

    // Runtime state
    @GuardedBy("lock")
    private long generation;
    @GuardedBy("lock")
    private boolean createdExecutorService;
    @GuardedBy("lock")
    private File generationFile;
    @GuardedBy("lock")
    private File lockFile;
    @GuardedBy("lock")
    private FileChannel lockFileChannel;
    @GuardedBy("lock")
    private File indxFile;
    @GuardedBy("lock")
    private File keysFile;
    @GuardedBy("lock")
    private File valsFile;
    @GuardedBy("lock")
    private File modsFile;
    @GuardedBy("lock")
    private FileOutputStream modsFileOutput;
    @GuardedBy("lock")
    private FileChannel directoryChannel;                               // not used on Windows
    @GuardedBy("lock")
    private long modsFileLength;
    @GuardedBy("lock")
    private long modsFileSyncPoint;
    @GuardedBy("lock")
    private ByteBuffer indx;
    @GuardedBy("lock")
    private ByteBuffer keys;
    @GuardedBy("lock")
    private ByteBuffer vals;
    @GuardedBy("lock")
    private ArrayKVStore kvstore;
    @GuardedBy("lock")
    private MutableView mods;
    @GuardedBy("lock")
    private Writes modsWritesSnapshot;
    @GuardedBy("lock")
    private Compaction compaction;
    @GuardedBy("lock")
    private long firstModTimestamp;
    @GuardedBy("lock")
    private long totalMillisWaiting;
    @GuardedBy("lock")
    private int hotCopiesInProgress;

// Accessors

    /**
     * Get the filesystem directory containing the database. If not set, this class functions as an in-memory store.
     *
     * @return database directory, or null for none
     */
    public File getDirectory() {
        this.readLock.lock();
        try {
            return this.directory;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Configure the filesystem directory containing the database. If not set, this class functions as an in-memory store.
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
     * Configure the {@link ScheduledExecutorService} used to schedule background compaction.
     *
     * <p>
     * If not explicitly configured, a {@link ScheduledExecutorService} will be created automatically during {@link #start}
     * using {@link Executors#newSingleThreadScheduledExecutor} and shutdown by {@link #stop} (if explicitly configured here,
     * the configured {@link ScheduledExecutorService} will not be shutdown by {@link #stop}).
     *
     * @param scheduledExecutorService schduled executor service, or null to have one created automatically
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.writeLock.lock();
        try {
            Preconditions.checkState(this.kvstore == null, "already started");
            this.scheduledExecutorService = scheduledExecutorService;
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
     * <p>
     * This value is applied to the size of the on-disk modifications file.
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
     * This value is applied to the size of the on-disk modifications file.
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

    /**
     * Get the total number of milliseconds spent in artificial delays caused by waiting for compaction.
     *
     * @return total delay in milliseconds
     */
    public long getTotalMillisWaiting() {
        this.readLock.lock();
        try {
            return this.totalMillisWaiting;
        } finally {
            this.readLock.unlock();
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
            this.log.info("starting {}", this);

            // Sanity check
            assert this.compaction == null;
            assert this.scheduledExecutorService == null;
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
            assert this.modsWritesSnapshot == null;
            assert this.firstModTimestamp == 0;
            assert this.totalMillisWaiting == 0;

            // Check configuration
            Preconditions.checkState(this.directory != null, "no directory configured");

            // Create executor if needed
            this.createdExecutorService = this.scheduledExecutorService == null;
            if (this.createdExecutorService) {
                this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(action -> {
                    final Thread thread = new Thread(action);
                    thread.setName("Compactor for " + this);
                    return thread;
                });
            }

            // Create directory if needed
            boolean initializeFiles = false;
            if (!this.directory.exists()) {
                if (!this.directory.mkdirs())
                    throw new ArrayKVException(String.format("failed to create directory \"%s\"", this.directory));
            }
            if (!this.directory.isDirectory())
                throw new ArrayKVException(String.format("file \"%ds\" is not a directory", this.directory));

            // Get directory channel we can fsync()
            try {
                this.directoryChannel = FileChannel.open(this.directory.toPath());
            } catch (IOException e) {
                if (!this.suckyOS)
                    throw e;
            }

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

                // Verify no index, keys, or values file exists
                try (DirectoryStream<Path> paths = Files.newDirectoryStream(this.directory.toPath())) {
                    for (Path path : paths) {
                        final File file = path.toFile();
                        final String name = file.getName();
                        if (name.startsWith(INDX_FILE_NAME_BASE)
                          || name.startsWith(KEYS_FILE_NAME_BASE)
                          || name.startsWith(VALS_FILE_NAME_BASE)) {
                            throw new ArrayKVException(String.format(
                              "database file inconsistency: found %s but not %s in %s",
                              name, GENERATION_FILE_NAME, this.directory));
                        }
                    }
                }

                // Create empty index, keys, and values files
                try (
                  FileOutputStream indxOutput = new FileOutputStream(new File(this.directory, INDX_FILE_NAME_BASE + 0));
                  FileOutputStream keysOutput = new FileOutputStream(new File(this.directory, KEYS_FILE_NAME_BASE + 0));
                  FileOutputStream valsOutput = new FileOutputStream(new File(this.directory, VALS_FILE_NAME_BASE + 0));
                  ArrayKVWriter arrayWriter = new ArrayKVWriter(indxOutput, keysOutput, valsOutput)) {
                    arrayWriter.flush();                                                        // avoid compiler warning
                    valsOutput.getChannel().force(false);
                    keysOutput.getChannel().force(false);
                    indxOutput.getChannel().force(false);
                }
                if (this.directoryChannel != null)
                    this.directoryChannel.force(false);

                // Create generation file
                try (FileOutputStream output = new FileOutputStream(this.generationFile)) {
                    output.write("0\n".getBytes(StandardCharsets.UTF_8));
                    output.flush();
                    output.getChannel().force(false);
                }
                if (this.directoryChannel != null)
                    this.directoryChannel.force(false);
            }

            // Read current generation number
            try (LineNumberReader reader = new LineNumberReader(
              new InputStreamReader(new FileInputStream(this.generationFile), StandardCharsets.UTF_8))) {
                final String line = reader.readLine();
                if (line == null)
                    throw new ArrayKVException(String.format("generation file %s is empty", this.generationFile));
                this.generation = Long.parseLong(line.trim(), 10);
                if (this.generation < 0)
                    throw new ArrayKVException(String.format("read negative generation number from %s", this.generationFile));
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
            try (DirectoryStream<Path> paths = Files.newDirectoryStream(this.directory.toPath())) {
                for (Path path : paths) {
                    final File file = path.toFile();
                    if (!expectedFiles.contains(file))
                        this.log.warn("ignoring unexpected file {} in my database directory", file.getName());
                }
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
            this.mods = new MutableView(this.kvstore, false);

            // Setup modifications file
            this.modsFileOutput = new FileOutputStream(this.modsFile, true);
            this.modsFileLength = this.modsFileOutput.getChannel().size();
            this.modsFileSyncPoint = this.modsFileLength;

            // Read and apply pre-existing uncompacted modifications from modifications file
            if (this.modsFileLength > 0) {
                this.log.info("reading {} bytes of uncompacted modifications from {}", this.modsFileLength, this.modsFile);
                try (FileInputStream input = new FileInputStream(this.modsFile)) {
                    while (input.available() > 0) {
                        final Writes writes;
                        try {
                            writes = Writes.deserialize(input, true);
                        } catch (Exception e) {
                            break;                                                      // probably a partial write
                        }
                        writes.applyTo(this.mods);
                    }
                }
                this.firstModTimestamp = System.nanoTime() | 1;                     // avoid zero value which is special
            }

            // Schedule compaction if necessary
            this.scheduleCompactionIfNecessary();

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
            this.log.info("stopping {}", this);

            // Cleanup
            this.cleanup();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void cleanup() {

        // Should hold write lock now
        assert this.lock.isWriteLockedByCurrentThread();

        // Cancel compaction if possible, otherwise wait for it to complete
        if (this.compaction != null && !this.compaction.cancel()) {

            // Wait for compaction to complete
            this.log.debug("waiting for in-progress compaction to complete before shutdown");
            this.compaction.waitForCompletion(0);
            this.log.debug("compaction completed, proceeding with shutdown");

            // Check whether another thread invoked stop() while we were asleep
            if (this.kvstore == null)
                return;
        }

        // Wait for any in-progress hot copies to complete
        if (this.hotCopiesInProgress > 0) {
            this.log.debug("waiting for {} hot copies to complete before shutdown", this.hotCopiesInProgress);
            boolean interrupted = false;
            do {

                // Wait for hot copies to complete
                try {
                    this.hotCopyFinishedCondition.await();
                } catch (InterruptedException e) {
                    this.log.warn("thread interrupted while waiting for "
                      + this.hotCopiesInProgress + " hot copies to complete (ignoring)", e);
                    interrupted = true;
                }

                // Check whether another thread invoked stop() while we were asleep
                if (this.kvstore == null)
                    return;
            } while (this.hotCopiesInProgress > 0);
            if (interrupted)
                Thread.currentThread().interrupt();
            this.log.debug("hot copies completed, proceeding with shutdown");
        }

        // Shut down executor - only if we created it
        if (this.createdExecutorService) {
            this.scheduledExecutorService.shutdownNow();
            this.scheduledExecutorService = null;
            this.createdExecutorService = false;
        }

        // Close files
        for (Closeable resource : new Closeable[] { this.modsFileOutput, this.directoryChannel, this.lockFileChannel }) {
            if (resource != null)
                this.closeIgnoreException(resource);
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
        this.modsWritesSnapshot = null;
        this.firstModTimestamp = 0;
        this.totalMillisWaiting = 0;
    }

// KVStore

    @Override
    public ByteData get(ByteData key) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.get(key);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.getAtLeast(minKey, maxKey);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.getAtMost(maxKey, minKey);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.getRange(minKey, maxKey, reverse);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void put(ByteData key, ByteData value) {
        final Writes writes = new Writes();
        writes.getPuts().put(key, value);
        this.apply(writes, false);
    }

    @Override
    public void remove(ByteData key) {
        final Writes writes = new Writes();
        writes.getRemoves().add(new KeyRange(key));
        this.apply(writes, false);
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        final Writes writes = new Writes();
        writes.getRemoves().add(new KeyRange(minKey != null ? minKey : ByteData.empty(), maxKey));
        this.apply(writes, false);
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        final Writes writes = new Writes();
        writes.getAdjusts().put(key, amount);
        this.apply(writes, false);
    }

    @Override
    public ByteData encodeCounter(long value) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.encodeCounter(value);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long decodeCounter(ByteData bytes) {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");
            return this.mods.decodeCounter(bytes);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void apply(Mutations mutations) {
        this.apply(mutations, false);
    }

// AtomicKVStore

    @Override
    public CloseableKVStore readOnlySnapshot() {
        this.readLock.lock();
        try {
            Preconditions.checkState(this.kvstore != null, "closed");

            // Clone the modifications currrently being compacted, if any
            Writes compactingWrites = null;
            if (this.mods.getBaseKVStore() instanceof MutableView) {                        // we are compacting
                assert this.compaction != null;
                final MutableView compactingMods = (MutableView)this.mods.getBaseKVStore();
                assert compactingMods.getBaseKVStore() == this.kvstore;
                synchronized (compactingMods) {
                    if (!compactingMods.getWrites().isEmpty())
                        compactingWrites = compactingMods.getWrites().readOnlySnapshot();
                }
            }

            // Clone the uncompacted modifications, if any, using cached snapshot if possible
            if (this.modsWritesSnapshot == null) {
                synchronized (this.mods) {
                    if (!this.mods.getWrites().isEmpty())
                        this.modsWritesSnapshot = this.mods.getWrites().readOnlySnapshot();
                }
            }
            final Writes outstandingWrites = this.modsWritesSnapshot;

            // Build snapshot by layering uncompacted modifications on top
            KVStore snapshot = this.kvstore;
            if (compactingWrites != null)
                snapshot = new MutableView(snapshot, compactingWrites);
            if (outstandingWrites != null)
                snapshot = new MutableView(snapshot, outstandingWrites);

            // Done
            return new CloseableForwardingKVStore(snapshot, null);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void apply(Mutations mutations, final boolean sync) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        this.writeLock.lock();
        try {

            // Verify we are started
            Preconditions.checkState(this.kvstore != null, "not started");

            // Delay operation if size of outstanding modifications is approaching high water mark
            while (this.compaction != null) {

                // Calculate how full is low -> high window
                final float windowRatio =
                   this.modsFileLength <= this.compactLowWater ? 0.0f :
                   this.modsFileLength >= this.compactHighWater ? 1.0f :
                   (float)(this.modsFileLength - this.compactLowWater) / (float)(this.compactHighWater - this.compactLowWater);

                // Calculate corresponding delay
                final long delayMillis = this.calculateCompactionPressureDelay(windowRatio);
                if (delayMillis < 0)
                    break;

                // If there is already a scheduled compaction, reschedule to be immediate
                if (this.compaction.getDelay() > 0)
                    this.scheduleCompaction(0);

                // Wait for it to complete
                final long delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
                long waitStart = 0;
                if (this.log.isDebugEnabled()) {
                    this.log.debug(String.format("reached %d%% of high-water mark; waiting up to %dms for in-progress"
                      + " compaction to complete before applying mutation(s)", (int)(windowRatio * 100), delayMillis));
                    waitStart = System.nanoTime();
                }
                this.compaction.waitForCompletion(delayNanos);
                if (this.log.isDebugEnabled()) {
                    this.log.debug(String.format("compaction completed after %dms, proceeding with application of mutation(s)",
                      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitStart)));
                }

                // Verify we are still open after giving up lock
                if (this.kvstore == null)
                    throw new ArrayKVException("k/v store was closed while waiting for compaction to complete");

                // Done waiting
                break;
            }

            // Get mutations as a Writes object
            final Writes writes;
            if (mutations instanceof Writes)
                writes = (Writes)mutations;
            else {
                writes = new Writes();
                try (Stream<KeyRange> removes = mutations.getRemoveRanges()) {
                    writes.getRemoves().add(new KeyRanges(removes));
                }
                try (Stream<Map.Entry<ByteData, ByteData>> puts = mutations.getPutPairs()) {
                    puts.iterator().forEachRemaining(entry -> writes.getPuts().put(entry.getKey(), entry.getValue()));
                }
                try (Stream<Map.Entry<ByteData, Long>> adjusts = mutations.getAdjustPairs()) {
                    adjusts.iterator().forEachRemaining(entry -> writes.getAdjusts().put(entry.getKey(), entry.getValue()));
                }
            }

            // Check for the trivial case where there are zero modifications
            if (writes.isEmpty())
                return;

            // Append mutations to uncompacted mods file
            try {
                final BufferedOutputStream buf = new BufferedOutputStream(this.modsFileOutput);
                writes.serialize(buf);
                buf.flush();
            } catch (IOException e) {
                this.log.error("error writing transaction mutations to {}", this.modsFile, e);

                // Attempt to recover by closing and re-opening the mods file, and discard our partially-written additions
                this.closeIgnoreException(this.modsFileOutput);
                try {
                    this.modsFileOutput = new FileOutputStream(this.modsFile, true);
                    this.modsFileOutput.getChannel().truncate(this.modsFileLength);              // undo append
                } catch (IOException e2) {
                    this.log.error("error reopening log file after write error - we're probably hosed", e2);
                }
                throw new ArrayKVException(String.format("error appending to %s", this.modsFile), e);
            }
            final long newModsFileLength;
            try {
                newModsFileLength = this.modsFileOutput.getChannel().size();
            } catch (IOException e) {
                throw new ArrayKVException(String.format("error getting length of %s", this.modsFile), e);
            }
            if (this.log.isDebugEnabled()) {
                this.log.debug("appended {} bytes to {} (new length {})",
                  newModsFileLength - this.modsFileLength, this.modsFile, newModsFileLength);
            }
            this.modsFileLength = newModsFileLength;

            // Apply removes
            try (Stream<KeyRange> removes = mutations.getRemoveRanges()) {
                removes.iterator().forEachRemaining(range -> {
                    final ByteData min = range.getMin();
                    final ByteData max = range.getMax();
                    this.mods.removeRange(min, max);
                    this.modsWritesSnapshot = null;
                });
            }

            // Apply puts
            try (Stream<Map.Entry<ByteData, ByteData>> puts = mutations.getPutPairs()) {
                puts.iterator().forEachRemaining(entry -> {
                    final ByteData key = entry.getKey();
                    final ByteData val = entry.getValue();
                    this.mods.put(key, val);
                    this.modsWritesSnapshot = null;
                });
            }

            // Apply counter adjustments
            try (Stream<Map.Entry<ByteData, Long>> adjusts = mutations.getAdjustPairs()) {
                adjusts.iterator().forEachRemaining(entry -> {
                    final ByteData key = entry.getKey();
                    final long adjust = entry.getValue();
                    this.mods.adjustCounter(key, adjust);
                    this.modsWritesSnapshot = null;
                });
            }

            // Set first uncompacted modification timestamp, if not already set
            if (this.firstModTimestamp == 0)
                this.firstModTimestamp = System.nanoTime() | 1;                 // avoid zero value which is special

            // Schedule compaction if necessary
            this.scheduleCompactionIfNecessary();

            // If we're not syncing, we're done
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

// Hot Copy

    /**
     * Create a filesystem atomic snapshot, or "hot" copy", of this instance in the specified destination directory.
     * The copy will be up-to-date as of the time this method is invoked.
     *
     * <p>
     * The {@code target} directory will be created if it does not exist; otherwise, it must be empty.
     * All files except the uncompacted modifications file are copied into {@code target}
     * in constant time using {@linkplain Files#createLink hard links} if possible.
     *
     * <p>
     * The hot copy operation can proceed in parallel with normal database activity, with the exception
     * that the compaction operation cannot start while there are concurrent hot copies in progress.
     *
     * <p>
     * Therefore, for best performance, consider {@linkplain #scheduleCompaction performing a compaction}
     * immediately prior to this operation.
     *
     * <p>
     * Note: when this method returns, all copied files, and the {@code target} directory, have been {@code fsync()}'d
     * and therefore may be considered durable in case of a system crash.
     *
     * @param target destination directory
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code target} exists and is not a directory or is non-empty
     * @throws IllegalArgumentException if {@code target} is null
     */
    public void hotCopy(File target) throws IOException {

        // Sanity check
        Preconditions.checkArgument(target != null, "null target");
        final Path dir = target.toPath();

        // Create/verify directory
        if (!Files.exists(dir))
            Files.createDirectories(dir);
        if (!Files.isDirectory(dir))
            throw new IllegalArgumentException(String.format("target \"%s\" is not a directory", dir));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path path : stream)
                throw new IllegalArgumentException(String.format("target \"%s\" is not empty", dir));
        }

        // Increment hot copy counter - this prevents compaction from removing files while we're copying them
        this.writeLock.lock();
        try {

            // Sanity check
            Preconditions.checkState(this.kvstore != null, "not started");

            // Bump counter
            this.hotCopiesInProgress++;
        } finally {
            this.writeLock.unlock();
        }
        try {

            // Logit
            this.log.debug("started hot copy into {}", target);

            // Copy index, keys, and values files using hard links (if possible) as these files are read-only
            final ArrayList<File> regularCopyFiles = new ArrayList<>(5);
            for (File file : new File[] { this.indxFile, this.keysFile, this.valsFile }) {
                try {
                    Files.createLink(dir.resolve(file.getName()), file.toPath());
                } catch (IOException | UnsupportedOperationException e) {
                    regularCopyFiles.add(file);                      // fall back to normal copy
                }
            }

            // Copy remaining files without using hard links
            regularCopyFiles.add(this.modsFile);                     // it's ok if we copy a partial write
            regularCopyFiles.add(this.generationFile);               // copy this one last
            for (File file : regularCopyFiles) {
                try (FileOutputStream fileCopy = new FileOutputStream(new File(target, file.getName()))) {
                    Files.copy(file.toPath(), fileCopy);
                    fileCopy.getChannel().force(false);
                }
            }

            // Sync directory
            try (FileChannel dirChannel = FileChannel.open(dir)) {
                dirChannel.force(false);
            } catch (IOException e) {
                if (!this.suckyOS)
                    throw e;
            }
        } finally {
            this.writeLock.lock();
            try {

                // Sanity check
                assert this.hotCopiesInProgress > 0;
                assert this.kvstore != null;

                // Logit
                this.log.debug("completed hot copy into {}", target);

                // Decrement counter
                this.hotCopiesInProgress--;

                // Wakeup waiters
                this.hotCopyFinishedCondition.signalAll();
            } finally {
                this.writeLock.unlock();
            }
        }
    }

// Compaction

    /**
     * Calculate the maximum amount of time that a thread attempting to {@link #apply(Mutations, boolean) apply()}
     * mutations must wait for a background compaction to complete as we start nearing the high water mark.
     *
     * <p>
     * The implementation in {@link AtomicArrayKVStore} returns {@code -1} for all values below 50%,
     * and then scales according to an inverse power function returning zero at 50%, 100ms at 75%,
     * and {@link Long#MAX_VALUE} at 100%.
     *
     * @param windowRatio the uncompacted mutation size as a fraction of the interval between low and high
     *  water marks; always a value value between zero and one (exclusive)
     * @return negative value for no delay, zero or positive value to trigger a background compaction
     *  (if not already running) and wait up to that many milliseconds for it to complete
     */
    protected long calculateCompactionPressureDelay(float windowRatio) {
        if ((windowRatio -= 0.5f) < 0)
            return -1;
        return (long)(100 * (1.0f / (0.5f - windowRatio) - 1.0f));
    }

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

            // Is there anything to compact?
            if (this.modsFileLength == 0)
                return null;

            // Schedule an immediate compaction
            return this.scheduleCompaction(0);
        } finally {
            this.writeLock.unlock();
        }
    }

    private Future<?> scheduleCompaction(long millis) {

        // Should hold write lock now
        assert this.lock.isWriteLockedByCurrentThread();
        assert this.modsFileLength > 0;

        // (Re)schedule compaction if necessary
        if (this.compaction == null || (this.compaction.getDelay() > millis && this.compaction.cancel())) {
            assert this.compaction == null;
            this.compaction = new Compaction(millis);
            if (this.log.isTraceEnabled())
                this.log.trace("scheduling compaction for {}ms from now", millis);
        }

        // Done
        return new ForwardingFuture.SimpleForwardingFuture<Void>(this.compaction.getFuture()) {
            @Override
            public boolean cancel(boolean mayInterrupt) {               // disallow this operation
                return false;
            }
        };
    }

    private void scheduleCompactionIfNecessary() {

        // Should hold write lock now
        assert this.lock.isWriteLockedByCurrentThread();

        // Is there anything to do?
        if (this.modsFileLength == 0)
            return;

        // Check space low-water mark
        if (this.modsFileLength > this.compactLowWater) {
            this.scheduleCompaction(0);
            return;
        }

        // Check maximum compaction delay
        if (this.firstModTimestamp != 0) {
            final long firstModAgeMillis = (System.nanoTime() - this.firstModTimestamp) / 1000000L;     // convert ns -> ms
            final long remainingDelay = Math.max(0, this.compactMaxDelay * 1000L - firstModAgeMillis);
            if (this.log.isTraceEnabled())
                this.log.trace("first modification was {}ms ago, remainingDelay = {}ms", firstModAgeMillis, remainingDelay);
            this.scheduleCompaction(remainingDelay);
            return;
        }
    }

    @SuppressWarnings("fallthrough")
    private void compact(final Compaction compaction) throws IOException {

        // Sanity check
        assert compaction != null;

        // Handle cancel() race condition
        this.writeLock.lock();
        try {

            // Were we canceled?
            if (compaction != this.compaction)
                return;

            // Set now running - this prevents future cancel()'s
            assert !compaction.isStarted();
            assert !compaction.isCompleted();
            compaction.setStarted();
        } finally {
            this.writeLock.unlock();
        }

        // Start compaction
        final long compactionStartTime;
        try {

            // Snapshot (and wrap) pending modifications, and mark log file position
            final Writes writesToCompact;
            final long previousModsFileLength;
            final long previousModsFileSyncPoint;
            this.writeLock.lock();
            try {

                // Sanity checks
                assert this.kvstore != null;
                assert this.modsFileLength > 0;

                // Mark start time and get uncompacted modifications
                compactionStartTime = System.nanoTime();
                writesToCompact = this.mods.getWrites();

                // It's possible the uncompacted modifications are a no-op; if so, no compaction is necessary
                if (writesToCompact.isEmpty()) {

                    // Wait for any in-progress hot copies to complete
                    while (this.hotCopiesInProgress > 0) {
                        this.log.debug("waiting for {} hot copies to complete before completing (trivial) compaction",
                          this.hotCopiesInProgress);
                        try {
                            this.hotCopyFinishedCondition.await();
                        } catch (InterruptedException e) {
                            throw new ArrayKVException(String.format(
                              "thread was interrupted while waiting for %d hot copies to complete", this.hotCopiesInProgress), e);
                        }
                        this.log.debug("hot copies completed, proceeding with completion of (trivial) compaction");
                    }
                    assert this.kvstore != null;
                    assert this.modsFileLength > 0;

                    // Discard outstanding mods
                    this.modsFileOutput.getChannel().truncate(0);
                    this.modsFileLength = 0;
                    this.modsFileSyncPoint = 0;
                    this.modsFileOutput.getChannel().force(false);
                    return;
                }

                // Allow new modifications to be added by other threads while we are compacting the old modifications
                this.mods = new MutableView(this.mods, false);
                this.modsWritesSnapshot = null;
                previousModsFileLength = this.modsFileLength;
                previousModsFileSyncPoint = this.modsFileSyncPoint;
            } finally {
                this.writeLock.unlock();
            }
            if (this.log.isDebugEnabled()) {
                this.log.debug("starting compaction for generation {} -> {} with mods file length {}",
                  this.generation, this.generation + 1, previousModsFileLength);
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
                  FileOutputStream indxOutput = new FileOutputStream(newIndxFile);
                  FileOutputStream keysOutput = new FileOutputStream(newKeysFile);
                  FileOutputStream valsOutput = new FileOutputStream(newValsFile);
                  ArrayKVWriter arrayWriter = new ArrayKVWriter(indxOutput, keysOutput, valsOutput)) {

                    // Write out merged key/value pairs
                    try (CloseableIterator<KVPair> i = this.kvstore.getRange(null, null)) {
                        arrayWriter.writeMerged(this.kvstore, i, writesToCompact);
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
                newModsFileOutput = new FileOutputStream(newModsFile, true);
                assert newModsFile.exists();

                // Sync directory
                if (this.directoryChannel != null)
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
                            this.log.debug("compaction for generation {} -> {} finishing up"
                              + " with {} bytes of new modifications after {} seconds",
                              this.generation, this.generation + 1, additionalModsLength, String.format("%.4f", duration));
                        }

                        // Wait for any in-progress hot copies to complete
                        while (this.hotCopiesInProgress > 0) {
                            this.log.debug("waiting for {} hot copies to complete before completing compaction",
                              this.hotCopiesInProgress);
                            try {
                                this.hotCopyFinishedCondition.await();
                            } catch (InterruptedException e) {
                                throw new ArrayKVException(String.format(
                                  "thread was interrupted while waiting for %d hot copies to complete",
                                  this.hotCopiesInProgress), e);
                            }
                            assert this.compaction == compaction;
                            assert this.kvstore != null;
                            this.log.debug("hot copies completed, proceeding with completion of compaction");
                        }

                        // Apply any changes that were made while we were unlocked and writing files
                        long newModsFileLength = 0;
                        long newModsFileSyncPoint = 0;
                        if (additionalModsLength > 0) {
                            try (FileChannel modsFileChannel = FileChannel.open(this.modsFile.toPath(), StandardOpenOption.READ)) {
                                final FileChannel newModsFileChannel = newModsFileOutput.getChannel();

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

                        // Atomically update new generation file contents, except on Windows where that's impossible
                        final FileOutputStream genOutput = !this.suckyOS ?
                          new AtomicUpdateFileOutputStream(this.generationFile) : new FileOutputStream(this.generationFile);
                        boolean genSuccess = false;
                        try {
                            genOutput.write((newGeneration + "\n").getBytes(StandardCharsets.UTF_8));
                            genOutput.flush();
                            genOutput.getChannel().force(false);
                            genSuccess = true;
                        } finally {
                            if (genSuccess)
                                genOutput.close();
                            else if (genOutput instanceof AtomicUpdateFileOutputStream)
                                ((AtomicUpdateFileOutputStream)genOutput).cancel();
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
                        newModsFileOutput = null;
                        this.modsFileLength = newModsFileLength;
                        this.modsFileSyncPoint = newModsFileSyncPoint;
                        this.kvstore = new ArrayKVStore(this.indx, this.keys, this.vals);
                        this.mods = new MutableView(this.kvstore, this.mods.getWrites());
                        this.modsWritesSnapshot = null;
                        if (additionalModsLength == 0)
                            this.firstModTimestamp = 0;
                        else {
                            this.firstModTimestamp = System.nanoTime() | 1;
                            this.scheduleCompactionIfNecessary();
                        }

                        // Sync directory prior to deleting files
                        if (this.directoryChannel != null) {
                            try {
                                this.directoryChannel.force(false);
                            } catch (IOException e) {
                                this.log.error("error syncing directory {} (ignoring)", this.directory, e);
                            }
                        }

                        // Close old mods file output stream
                        this.closeIgnoreException(oldModsFileOutput);

                        // Delete old files
                        this.deleteWarnException(oldIndxFile);
                        this.deleteWarnException(oldKeysFile);
                        this.deleteWarnException(oldValsFile);
                        this.deleteWarnException(oldModsFile);
                    }
                } finally {
                    try {

                        // Logit
                        if (this.log.isDebugEnabled()) {
                            final float duration = (System.nanoTime() - compactionStartTime) / 1000000000f;
                            this.log.debug("compaction for generation {} -> {} {} in {} seconds", newGeneration - 1, newGeneration,
                              success ? "successfully compacted " + previousModsFileLength + " bytes" : "failed",
                              String.format("%.4f", duration));
                        }

                        // Cleanup/undo on failure
                        if (newModsFileOutput != null) {
                            this.closeIgnoreException(newModsFileOutput);
                            this.deleteWarnException(newModsFile);
                        }
                        if (!success) {

                            // Put back the old uncompacted modifications, and merge any new mods into them
                            final Writes writesDuringCompaction = this.mods.getWrites();
                            this.mods = new MutableView(this.kvstore, writesToCompact);
                            this.modsWritesSnapshot = null;
                            writesDuringCompaction.applyTo(this.mods);

                            // Delete the files we were creating
                            this.deleteWarnException(newIndxFile);
                            this.deleteWarnException(newKeysFile);
                            this.deleteWarnException(newValsFile);
                        }
                    } finally {
                        this.writeLock.unlock();
                    }
                }
            }
        } finally {

            // Update state
            this.writeLock.lock();
            try {
                assert this.kvstore != null;
                assert this.compaction == compaction;
                compaction.setCompleted();
                this.compaction = null;
            } finally {
                this.writeLock.unlock();
            }
        }
    }

    private boolean isWindows() {
        return System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).contains("win");
    }

    private void deleteWarnException(File file) {
        try {
            Files.delete(file.toPath());
        } catch (IOException e) {
            this.log.warn("error deleting {} (proceeding anyway): {}", file, e.toString());
        }
    }

    private void closeIgnoreException(Closeable resource) {
        try {
            resource.close();
        } catch (IOException e) {
            // ignore
        }
    }

// Compaction

    private class Compaction implements Runnable {

        private final Condition completedCondition = AtomicArrayKVStore.this.writeLock.newCondition();
        private final ScheduledFuture<Void> future;

        private boolean started;
        private boolean completed;

        @SuppressWarnings("unchecked")
        Compaction(long millis) {

            // Sanity check
            assert AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();
            Preconditions.checkState(AtomicArrayKVStore.this.compaction == null, "compaction already exists");

            // Schedule
            this.future = (ScheduledFuture<Void>)AtomicArrayKVStore.this.scheduledExecutorService.schedule(this,
              millis, TimeUnit.MILLISECONDS);
        }

        /**
         * Cancel this compaction.
         *
         * @return true if canceled, false if already started
         */
        public boolean cancel() {

            // Sanity check
            assert AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();
            Preconditions.checkState(this.future != null, "not scheduled");

            // Already canceled?
            if (AtomicArrayKVStore.this.compaction != this)
                return false;

            // Already started?
            if (this.started)
                return false;

            // Attempt to cancel scheduled task
            this.future.cancel(false);

            // Make this task do nothing even if it runs anyway
            AtomicArrayKVStore.this.compaction = null;
            return true;
        }

        /**
         * Get delay until start.
         *
         * @return delay in milliseconds
         */
        public long getDelay() {

            // Sanity check
            assert AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();
            Preconditions.checkState(this.future != null, "not scheduled");

            // Get delay
            return Math.max(0, this.future.getDelay(TimeUnit.MILLISECONDS));
        }

        /**
         * Wait for this compaction to complete.
         *
         * @param nanos max wait in nanoseconds, or zero for indefinite wait
         * @return true if compaction completed, false if timeout occurred first
         */
        public boolean waitForCompletion(long nanos) {

            // Sanity check
            assert AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();

            // Wait for completion
            boolean interrupted = false;
            long totalNanosWaiting = 0;
            long mark = System.nanoTime();
            while (!this.completed) {
                try {
                    if (nanos > 0) {
                        if ((nanos = this.completedCondition.awaitNanos(nanos)) <= 0)
                            break;
                    } else
                        this.completedCondition.await();
                } catch (InterruptedException e) {
                    AtomicArrayKVStore.this.log.warn(
                      "thread was interrupted while waiting for compaction to complete (ignoring)", e);
                    interrupted = true;
                } finally {
                    final long newMark = System.nanoTime();
                    totalNanosWaiting += newMark - mark;
                    mark = newMark;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            AtomicArrayKVStore.this.totalMillisWaiting += totalNanosWaiting / 1000000L;
            return this.completed;
        }

        public Future<Void> getFuture() {
            return this.future;
        }

        public boolean isStarted() {
            assert AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();
            return this.started;
        }
        public void setStarted() {
            assert AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();
            this.started = true;
        }

        public boolean isCompleted() {
            assert AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();
            return this.completed;
        }
        public void setCompleted() {
            assert AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();
            this.completed = true;
            this.completedCondition.signalAll();
        }

        @Override
        public void run() {
            assert !AtomicArrayKVStore.this.lock.isWriteLockedByCurrentThread();
            try {
                AtomicArrayKVStore.this.compact(this);
            } catch (Throwable t) {
                AtomicArrayKVStore.this.log.error("error during compaction", t);
            }
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
