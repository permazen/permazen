
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.fallback;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.raft.Consistency;
import io.permazen.kv.raft.RaftKVDatabase;
import io.permazen.kv.raft.RaftKVTransaction;
import io.permazen.kv.raft.Timestamp;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition-tolerant {@link KVDatabase} that automatically migrates between a clustered {@link RaftKVDatabase}
 * and a local, non-clustered "standalone mode" {@link KVDatabase}, based on availability of the Raft cluster.
 *
 * <p>
 * A {@link RaftKVDatabase} requires that the local node be part of a cluster majority, otherwise, transactions
 * cannot commit (even read-only ones) and application progress halts. This class adds partition tolerance to
 * a {@link RaftKVDatabase}, by maintaining a separate private "standalone mode" {@link KVDatabase} that can be used
 * in lieu of the normal {@link RaftKVDatabase} when the Raft cluster is unavailable.
 *
 * <p>
 * Instances transparently and automatically switch over to standalone mode {@link KVDatabase} when they determine
 * that the {@link RaftKVDatabase} is unavailable, and automatically switch back to using the {@link RaftKVDatabase}
 * once it is available again. The rate of switching is limited by an enforced hysteresis; see
 * {@link FallbackTarget#getMinAvailableTime} and {@link FallbackTarget#getMinUnavailableTime}.
 *
 * <p>
 * Of course, this sacrifices consistency. To address that, a configurable {@link MergeStrategy} is used to migrate the data
 * when switching between normal mode and standalone mode. The {@link MergeStrategy} is given read-only access to the
 * database being switched away from, and read-write access to the database being switched to; when switching away from
 * the {@link RaftKVDatabase}, {@link Consistency#EVENTUAL_COMMITTED} is used to eliminate the requirement for
 * communication with the rest of the cluster.
 *
 * <p>
 * Although more exotic, instances support migrating between multiple {@link RaftKVDatabase}s in a prioritized list.
 * For example, the local node may be part of two independent {@link RaftKVDatabase} clusters: a higher priority one
 * containing every node, and a lower priority one containing only nodes in the same data center as the local node.
 * In any case, the "standalone mode" database (which is not a clustered database) always has the lowest priority.
 *
 * <p>
 * Raft cluster availability is determined by {@link FallbackTarget#checkAvailability}; subclasses may override the default
 * implementation if desired.
 */
public class FallbackKVDatabase implements KVDatabase {

    private static final int MIGRATION_CHECK_INTERVAL = 1000;                   // every 1 second
    private static final int STATE_FILE_COOKIE = 0xe2bd1a96;
    private static final int CURRENT_FORMAT_VERSION = 1;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Configured state
    @GuardedBy("this")
    private final ArrayList<FallbackTarget> targets = new ArrayList<>();
    @GuardedBy("this")
    private File stateFile;
    @GuardedBy("this")
    private KVDatabase standaloneKV;
    @GuardedBy("this")
    private int initialTargetIndex = Integer.MAX_VALUE;
    @GuardedBy("this")
    private int maximumTargetIndex = Integer.MAX_VALUE;
    @GuardedBy("this")
    private int threadPriority = -1;

    // Runtime state
    @GuardedBy("this")
    private boolean migrating;
    @GuardedBy("this")
    private int migrationCount;
    @GuardedBy("this")
    private ScheduledExecutorService executor;
    @GuardedBy("this")
    private ScheduledFuture<?> migrationCheckFuture;
    @GuardedBy("this")
    private int startCount;
    @GuardedBy("this")
    private boolean started;
    @GuardedBy("this")
    private int currentTargetIndex;                     // index of current target, or -1 in standalone mode
    @GuardedBy("this")
    private Date lastStandaloneActiveTime;
    @GuardedBy("this")
    private final HashSet<FallbackFuture> futures = new HashSet<>();

// Methods

    /**
     * Get this instance's persistent state file.
     *
     * @return file for persistent state
     */
    public synchronized File getStateFile() {
        return this.stateFile;
    }

    /**
     * Configure this instance's persistent state file.
     *
     * <p>
     * Required property.
     *
     * @param stateFile file for persistent state
     * @throws IllegalArgumentException if {@code stateFile} is null
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setStateFile(File stateFile) {
        Preconditions.checkArgument(stateFile != null, "null stateFile");
        Preconditions.checkState(!this.started, "already started");
        this.stateFile = stateFile;
    }

    /**
     * Get the configured "standalone mode" {@link KVDatabase} to be used when all {@link FallbackTarget}s are unavailable.
     *
     * @return "standalone mode" database
     */
    public synchronized KVDatabase getStandaloneTarget() {
        return this.standaloneKV;
    }

    /**
     * Configure the local "standalone mode" {@link KVDatabase} to be used when all {@link FallbackTarget}s are unavailable.
     *
     * @param standaloneKV "standalone mode" database
     * @throws IllegalArgumentException if {@code standaloneKV} is null
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setStandaloneTarget(KVDatabase standaloneKV) {
        Preconditions.checkArgument(standaloneKV != null, "null standaloneKV");
        Preconditions.checkState(!this.started, "already started");
        this.standaloneKV = standaloneKV;
    }

    /**
     * Get most preferred {@link FallbackTarget}.
     *
     * <p>
     * Targets will be sorted in order of increasing preference.
     *
     * @return top fallback target, or null if none are configured yet
     */
    public synchronized FallbackTarget getFallbackTarget() {
        return !this.targets.isEmpty() ? this.targets.get(this.targets.size() - 1) : null;
    }

    /**
     * Get the {@link FallbackTarget}(s).
     *
     * <p>
     * Targets will be sorted in order of increasing preference.
     *
     * @return list of one or more fallback targets; the returned list is a snapshot-in-time copy of each target
     */
    public synchronized List<FallbackTarget> getFallbackTargets() {
        final ArrayList<FallbackTarget> result = new ArrayList<>(this.targets);
        for (int i = 0; i < result.size(); i++)
            result.set(i, result.get(i).clone());
        return result;
    }

    /**
     * Configure a single {@link FallbackTarget}.
     *
     * @param target fallback target
     * @throws IllegalArgumentException if {@code target} is null
     * @throws IllegalArgumentException if any target does not have a {@link RaftKVDatabase} configured
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setFallbackTarget(FallbackTarget target) {
        this.setFallbackTargets(Collections.singletonList(target));
    }

    /**
     * Configure multiple {@link FallbackTarget}(s).
     *
     * <p>
     * Targets should be sorted in order of increasing preference.
     *
     * @param targets targets in order of increasing preference
     * @throws IllegalArgumentException if {@code targets} is null
     * @throws IllegalArgumentException if {@code targets} is empty
     * @throws IllegalArgumentException if any target is null
     * @throws IllegalArgumentException if any target does not have a {@link RaftKVDatabase} configured
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setFallbackTargets(List<? extends FallbackTarget> targets) {
        Preconditions.checkArgument(targets != null, "null targets");
        Preconditions.checkArgument(!targets.isEmpty(), "empty targets");
        Preconditions.checkState(!this.started, "already started");
        this.targets.clear();
        for (FallbackTarget target : targets) {
            Preconditions.checkArgument(target != null, "null target");
            Preconditions.checkArgument(target.getRaftKVDatabase() != null, "target with no database configured");
            this.targets.add(target.clone());
        }
    }

    /**
     * Get the configured target index to use when starting up for the very first time.
     *
     * @return initial target index, with -1 meaning standalone mode
     */
    public synchronized int getInitialTargetIndex() {
        return this.initialTargetIndex;
    }

    /**
     * Configure the index of the currently active database when starting up for the very first time.
     * This value is only used on the initial startup; after that, the current fallback target is
     * persisted across restarts.
     *
     * <p>
     * Default value is the most highly preferred target. Use -1 to change the default to standalone mode;
     * this is appropriate when the Raft cluster is not yet configured on initial startup.
     *
     * @param initialTargetIndex initial target index, -1 meaning standalone mode; out of range values will be clipped
     */
    public synchronized void setInitialTargetIndex(int initialTargetIndex) {
        this.initialTargetIndex = initialTargetIndex;
    }

    /**
     * Get the index of the currently active database.
     *
     * @return index into fallback target list, or -1 for standalone mode
     */
    public synchronized int getCurrentTargetIndex() {
        return this.currentTargetIndex;
    }

    /**
     * Configure the maximum allowed target index. This is a dynamic control that can be changed at runtime
     * to force this instance into a lower index target (or standalone mode) than it would otherwise be in.
     *
     * <p>
     * Default value is {@link Integer#MAX_VALUE}.
     *
     * @param maximumTargetIndex maximum target index, -1 meaning standalone mode; out of range values will be clipped
     */
    public synchronized void setMaximumTargetIndex(int maximumTargetIndex) {
        maximumTargetIndex = Math.max(-1, maximumTargetIndex);
        if (this.maximumTargetIndex != maximumTargetIndex) {
            this.log.info("adjusting maximum target index to {}", maximumTargetIndex);
            this.maximumTargetIndex = maximumTargetIndex;
            this.executor.submit(new MigrationCheckTask());
        }
    }

    /**
     * Get the maximum allowed target index.
     *
     * @return maximum allowed target index; -1 for standalone mode
     */
    public synchronized int getMaximumTargetIndex() {
        return this.maximumTargetIndex;
    }

    /**
     * Get the last time the standalone database was active.
     *
     * @return last active time of the standalone database, or null if never active
     */
    public synchronized Date getLastStandaloneActiveTime() {
        return (Date)this.lastStandaloneActiveTime.clone();
    }

    /**
     * Configure the priority of the internal service thread.
     *
     * <p>
     * Default is -1, which means do not change thread priority from its default.
     *
     * @param threadPriority internal service thread priority, or -1 to leave thread priority unchanged
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if {@code threadPriority} is not -1 and not in the range
     *  {@link Thread#MIN_PRIORITY} to {@link Thread#MAX_PRIORITY}
     */
    public synchronized void setThreadPriority(int threadPriority) {
        Preconditions.checkArgument(threadPriority == -1
          || (threadPriority >= Thread.MIN_PRIORITY && threadPriority <= Thread.MAX_PRIORITY), "invalid threadPriority");
        Preconditions.checkState(!this.started, "already started");
        this.threadPriority = threadPriority;
    }

    /**
     * Get the configured internal service thread priority.
     *
     * @return internal service thread priority, or -1 if not configured
     */
    public synchronized int getThreadPriority() {
        return this.threadPriority;
    }

// KVDatabase

    @Override
    @PostConstruct
    public synchronized void start() {

        // Already started?
        if (this.started)
            return;
        this.startCount++;

        // Sanity check
        Preconditions.checkState(this.stateFile != null, "no state file configured");
        Preconditions.checkState(this.standaloneKV != null, "no standaloneKV configured");
        Preconditions.checkState(!this.targets.isEmpty(), "no targets configured");
        try {

            // Logging
            if (this.log.isDebugEnabled())
                this.log.info("starting up {}", this);

            // Create service thread pool
            final AtomicInteger threadId = new AtomicInteger();
            this.executor = Executors.newScheduledThreadPool(this.targets.size(),
              runnable -> this.createExecutorThread(runnable, threadId.incrementAndGet()));

            // Start underlying databases
            this.standaloneKV.start();
            for (FallbackTarget target : this.targets)
                target.getRaftKVDatabase().start();

            // Initialize my state (note: some may get overwritten by readStateFile())
            this.migrating = false;
            this.lastStandaloneActiveTime = null;
            this.currentTargetIndex = Math.max(-1, Math.min(this.targets.size() - 1, this.initialTargetIndex));

            // Perform initial availability checks and initialize target runtime state
            for (FallbackTarget target : this.targets) {
                this.log.info("performing initial availability check for {}", target);
                target.available = false;
                try {
                    target.available = target.checkAvailability(this);
                } catch (Exception e) {
                    if (this.log.isTraceEnabled())
                        this.log.trace("checkAvailable() for {} threw exception", target, e);
                    else if (this.log.isDebugEnabled())
                        this.log.debug("checkAvailable() for {} threw exception: {}", target, e.toString());
                }
                this.log.info("{} is initially {}available", target, target.available ? "" : "un");
                target.lastChangeTimestamp = null;
            }

            // Start periodic availability checks
            for (FallbackTarget target : this.targets) {
                target.future = this.executor.scheduleWithFixedDelay(new AvailabilityCheckTask(target),
                  target.getCheckInterval(), target.getCheckInterval(), TimeUnit.MILLISECONDS);
            }

            // Read state file, if present
            if (this.stateFile.exists()) {
                try {
                    this.readStateFile();
                } catch (IOException e) {
                    throw new RuntimeException("error reading persistent state file " + this.stateFile, e);
                }
            }

            // Set up periodic migration checks
            this.migrationCheckFuture = this.executor.scheduleWithFixedDelay(
              new MigrationCheckTask(), 0, MIGRATION_CHECK_INTERVAL, TimeUnit.MILLISECONDS);

            // Done
            this.started = true;
        } finally {
            if (!this.started)
                this.cleanup();
        }
    }

    @Override
    @PreDestroy
    public synchronized void stop() {

        // Already stopped?
        if (!this.started)
            return;
        this.cleanup();
    }

    /**
     * Create the service thread used by this instance.
     *
     * <p>
     * The implementation in {@link FallbackKVDatabase} simply instantiates a thread and sets the name.
     * Subclasses may override to set priority, etc.
     *
     * @param action thread entry point
     * @param uniqueId unique ID for the thread which increments each time this instance is (re)started
     * @return service thread for this instance
     */
    protected Thread createExecutorThread(Runnable action, int uniqueId) {
        final Thread thread = new Thread(action);
        thread.setName("Executor#" + uniqueId + " for " + FallbackKVDatabase.class.getSimpleName());
        synchronized (this) {
            if (this.threadPriority != -1)
                thread.setPriority(this.threadPriority);
        }
        return thread;
    }

    private void cleanup() {

        // Sanity check
        assert Thread.holdsLock(this);

        // Logging
        if (this.log.isDebugEnabled())
            this.log.info("shutting down {}", this);

        // Wait for migration to complete
        if (this.migrating) {
            if (this.log.isDebugEnabled())
                this.log.info("waiting for migration to finish to shut down {}", this);
            do {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    this.log.warn("interrupted during {} shutdown while waiting for migration to finish (ignoring)", this, e);
                }
                if (!this.started)              // we lost a race with another thread invoking stop()
                    return;
            } while (this.migrating);
            if (this.log.isDebugEnabled())
                this.log.info("migration finished, continuing with shut down of {}", this);
        }

        // Reset target runtime state
        for (FallbackTarget target : this.targets) {
            target.available = false;
            target.lastChangeTimestamp = null;
        }

        // Stop periodic checks
        this.targets.stream()
          .filter(target -> target.future != null)
          .iterator()
          .forEachRemaining(target -> {
            target.future.cancel(true);
            target.future = null;
          });
        if (this.migrationCheckFuture != null) {
            this.migrationCheckFuture.cancel(true);
            this.migrationCheckFuture = null;
        }

        // Shut down thread pool
        if (this.executor != null) {
            this.executor.shutdownNow();
            this.executor = null;
        }

        // Stop databases
        for (FallbackTarget target : this.targets) {
            try {
                target.getRaftKVDatabase().stop();
            } catch (Exception e) {
                this.log.warn("error stopping database target {} (ignoring)", this, e);
            }
        }
        try {
            this.standaloneKV.stop();
        } catch (Exception e) {
            this.log.warn("error stopping fallback database {} (ignoring)", this.standaloneKV, e);
        }

        // Done
        this.started = false;
    }

    @Override
    public FallbackKVTransaction createTransaction() {
        return this.createTransaction(null);
    }

    @Override
    public synchronized FallbackKVTransaction createTransaction(Map<String, ?> options) {

        // Sanity check
        Preconditions.checkState(this.started, "not started");

        // Create inner transaction from current database
        final KVDatabase currentKV = this.currentTargetIndex == -1 ?
          this.standaloneKV : this.targets.get(this.currentTargetIndex).getRaftKVDatabase();
        KVTransaction tx = currentKV.createTransaction(options);

        // Wrap it
        return new FallbackKVTransaction(this, tx, this.migrationCount);
    }

    /**
     * Subclass hook to veto an impending migration. This is invoked just prior to starting a migration.
     * If this method returns false, the migration is deferred.
     *
     * <p>
     * The implementation in {@link FallbackKVDatabase} always returns true.
     *
     * @param currTargetIndex current fallback target list index (before migration), or -1 for standalone mode
     * @param nextTargetIndex next fallback target list index (after migration), or -1 for standalone mode
     * @return true to allow migration to proceed, false to defer migration until later
     */
    protected boolean isMigrationAllowed(int currTargetIndex, int nextTargetIndex) {
        return true;
    }

    /**
     * Subclass hook to be notified when a migration occurs. This method is invoked after each successful target change.
     *
     * <p>
     * The implementation in {@link FallbackKVDatabase} does nothing.
     *
     * @param prevTargetIndex previous fallback target list index, or -1 for standalone mode
     * @param currTargetIndex current fallback target list index, or -1 for standalone mode
     */
    protected void migrationCompleted(int prevTargetIndex, int currTargetIndex) {
    }

// Object

    @Override
    public synchronized String toString() {
        return this.getClass().getSimpleName()
          + "[standalone=" + this.standaloneKV
          + ",targets=" + this.targets + "]";
    }

// Package methods

    synchronized boolean checkNoMigration(int migrationCount) {
        return !this.migrating && migrationCount == this.migrationCount;
    }

    synchronized boolean registerFallbackFutures(List<FallbackFuture> futureList, int migrationCount) {

        // Check freshness
        if (!this.checkNoMigration(migrationCount))
            return false;

        // Record these futures
        FallbackKVDatabase.this.futures.addAll(futureList);
        return true;
    }

// Internal methods

    // Perform availability check on the specified target
    private void performCheck(FallbackTarget target, final int startCount) {

        // Should NOT be locked when invoked
        assert !Thread.holdsLock(this);

        // Check for shutdown race condition
        final boolean wasAvailable;
        synchronized (this) {
            if (!this.started || startCount != this.startCount)
                return;
            wasAvailable = target.available;
        }

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("performing availability check for {} (currently {}available)", target, wasAvailable ? "" : "un");

        // Perform check
        boolean available = false;
        try {
            available = target.checkAvailability(this);
        } catch (Exception e) {
            if (this.log.isTraceEnabled())
                this.log.trace("checkAvailable() for {} threw exception", target, e);
            else if (wasAvailable && this.log.isDebugEnabled())
                this.log.debug("checkAvailable() for {} threw exception: {}", target, e.toString());
        }

        // Handle result
        synchronized (this) {

            // Check for shutdown race condition
            if (!this.started || startCount != this.startCount)
                return;

            // Prevent timestamp roll-over
            if (target.lastChangeTimestamp != null && target.lastChangeTimestamp.isRolloverDanger())
                target.lastChangeTimestamp = null;

            // Any state change?
            if (available == target.available)
                return;

            // Update availability and schedule an immediate migration check
            target.available = available;
            target.lastChangeTimestamp = new Timestamp();
            this.executor.submit(new MigrationCheckTask());
        }

        // Log result
        this.log.info("{} has become {}available", target, available ? "" : "un");
    }

    // Perform migration if necessary
    private void checkMigration(final int startCount) {
        final int currIndex;
        int bestIndex;
        final FallbackTarget currTarget;
        final FallbackTarget bestTarget;
        synchronized (this) {

            // Check for shutdown race condition
            if (!this.started || startCount != this.startCount)
                return;

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("performing migration check");

            // Allow only one migration at a time
            if (this.migrating) {
                if (this.log.isTraceEnabled())
                    this.log.trace("migration check canceled: migration in progress");
                return;
            }

            // Get the highest priority (i.e., best choice) database that is currently available
            bestIndex = Math.max(-1, Math.min(this.maximumTargetIndex, this.targets.size() - 1));
            while (bestIndex >= 0) {
                final FallbackTarget target = this.targets.get(bestIndex);

                // Enforce hysteresis: don't change state unless sufficient time has past since target's last state change
                final boolean previousAvailable = bestIndex >= this.currentTargetIndex;
                final boolean currentAvailable = target.available;
                final int timeSinceChange = target.lastChangeTimestamp != null ?
                  -target.lastChangeTimestamp.offsetFromNow() : Integer.MAX_VALUE;
                final boolean hysteresisAvailable;
                if (currentAvailable)
                    hysteresisAvailable = previousAvailable || timeSinceChange >= target.getMinAvailableTime();
                else
                    hysteresisAvailable = previousAvailable && timeSinceChange < target.getMinUnavailableTime();
                if (this.log.isTraceEnabled()) {
                    this.log.trace("{} availability: previous={}, current={}, hysteresis={}",
                      target, previousAvailable, currentAvailable, hysteresisAvailable);
                }

                // If this target is available, use it
                if (hysteresisAvailable)
                    break;

                // Try next best one
                bestIndex--;
            }

            // Already there?
            currIndex = this.currentTargetIndex;
            if (currIndex == bestIndex)
                return;

            // Get targets
            currTarget = currIndex != -1 ? this.targets.get(currIndex) : null;
            bestTarget = bestIndex != -1 ? this.targets.get(bestIndex) : null;

            // Ask subclass if migration is allowed right now
            if (!this.isMigrationAllowed(currIndex, bestIndex)) {
                if (this.log.isTraceEnabled())
                    this.log.trace("migration canceled: denied by {}.isMigrationAllowed()", this.getClass().getSimpleName());
                return;
            }

            // Start migration
            this.migrating = true;
        }
        final FallbackFuture[] oldFutures;
        try {
            final String desc = "migration from "
              + (currIndex != -1 ? "fallback target #" + currIndex : "standalone database")
              + " to "
              + (bestIndex != -1 ? "fallback target #" + bestIndex : "standalone database");
            try {

                // Gather info
                final KVDatabase currKV;
                final KVDatabase bestKV;
                final Date lastActiveTime;
                synchronized (this) {
                    currKV = currTarget != null ? currTarget.getRaftKVDatabase() : this.standaloneKV;
                    bestKV = bestTarget != null ? bestTarget.getRaftKVDatabase() : this.standaloneKV;
                    lastActiveTime = bestTarget != null ? bestTarget.lastActiveTime : this.lastStandaloneActiveTime;
                }
                final MergeStrategy mergeStrategy = bestIndex < currIndex ?
                  currTarget.getUnavailableMergeStrategy() : bestTarget.getRejoinMergeStrategy();

                // Logit
                this.log.info("starting fallback {} using {}", desc, mergeStrategy);

                // Create source transaction. Note the combination of read-only and EVENTUAL_COMMITTED is important, because this
                // guarantees that the transaction will generate no network traffic (and not require any majority) on commit().
                final KVTransaction src = currKV instanceof RaftKVDatabase ?
                  this.createSourceTransaction((RaftKVDatabase)currKV) : currKV.createTransaction();
                try {

                    // Create destination transaction
                    final KVTransaction dst = bestKV instanceof RaftKVDatabase ?
                      this.createDestinationTransaction((RaftKVDatabase)bestKV) : bestKV.createTransaction();
                    try {

                        // Get timestamp
                        final Date currentTime = new Date();

                        // Perform merge
                        mergeStrategy.mergeAndCommit(src, dst, lastActiveTime);

                        // Redirect new transactions
                        this.log.info(desc + " succeeded");
                        synchronized (this) {
                            if (currTarget != null)
                                currTarget.lastActiveTime = currentTime;
                            else
                                this.lastStandaloneActiveTime = currentTime;
                            this.currentTargetIndex = bestIndex;
                            this.migrationCount++;
                        }

                        // Notify subclass
                        this.migrationCompleted(currIndex, bestIndex);
                    } finally {
                        dst.rollback();                 // no effect if already committed
                    }
                } finally {
                    src.rollback();                     // no effect if already committed
                }
            } catch (RetryKVTransactionException e) {
                this.log.info("{} failed (will try again later): {}", desc, e.toString());
            } catch (Throwable t) {
                this.log.error(desc + " failed", t);
            }
        } finally {
            synchronized (this) {
                this.migrating = false;
                this.notifyAll();
                oldFutures = this.futures.toArray(new FallbackFuture[this.futures.size()]);
                this.futures.clear();
            }
        }

        // Update state file
        synchronized (this) {
            try {
                this.writeStateFile();
            } catch (IOException e) {
                this.log.error("error writing state to state file {}", this.stateFile, e);
            }
        }

        // Trigger spurious notifications for all futures associated with previous target
        for (FallbackFuture future : oldFutures)
            future.set(null);
    }

    /**
     * Create a Raft source transaction.
     *
     * <p>
     * The implementation in {@link FallbackKVDatabase} returns a new read-only transaction with consistency
     * {@link Consistency#EVENTUAL_COMMITTED}. The combination of read-only and {@link Consistency#EVENTUAL_COMMITTED}
     * is important, because this guarantees that the transaction will generate no network traffic (and not require
     * any majority to exist) on {@code commit()}.
     *
     * @param kvdb Raft database
     * @return new transaction for availability check
     */
    protected RaftKVTransaction createSourceTransaction(RaftKVDatabase kvdb) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        final RaftKVTransaction tx = kvdb.createTransaction(Consistency.EVENTUAL_COMMITTED);
        tx.setReadOnly(true);
        return tx;
    }

    /**
     * Create a Raft destination transaction.
     *
     * <p>
     * The implementation in {@link FallbackKVDatabase} just delegates to {@link RaftKVDatabase#createTransaction()}.
     *
     * @param kvdb Raft database
     * @return new transaction for availability check
     */
    protected RaftKVTransaction createDestinationTransaction(RaftKVDatabase kvdb) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        return kvdb.createTransaction();
    }

    /**
     * Create a Raft availability check transaction.
     *
     * <p>
     * The implementation in {@link FallbackKVDatabase} just delegates to {@link RaftKVDatabase#createTransaction()}.
     *
     * @param kvdb Raft database
     * @return new transaction for availability check
     */
    protected RaftKVTransaction createAvailabilityCheckTransaction(RaftKVDatabase kvdb) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        return kvdb.createTransaction();
    }

    private void readStateFile() throws IOException {

        // Sanity check
        assert Thread.holdsLock(this);

        // Read data
        final int targetIndex;
        final long standaloneActiveTime;
        final long[] lastActiveTimes;
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(new FileInputStream(this.stateFile)))) {
            final int cookie = input.readInt();
            if (cookie != STATE_FILE_COOKIE)
                throw new IOException("invalid state file " + this.stateFile + " (incorrect header)");
            final int formatVersion = input.readInt();
            switch (formatVersion) {
            case CURRENT_FORMAT_VERSION:
                break;
            default:
                throw new IOException("invalid state file " + this.stateFile + " format version (expecting "
                  + CURRENT_FORMAT_VERSION + ", found " + formatVersion + ")");
            }
            final int numTargets = input.readInt();
            if (numTargets != this.targets.size()) {
                this.log.warn("state file {} lists {} != {}, assuming configuration change and ignoring file",
                  this.stateFile, numTargets, this.targets.size());
                return;
            }
            targetIndex = input.readInt();
            if (targetIndex < -1 || targetIndex >= this.targets.size())
                throw new IOException("invalid state file " + this.stateFile + " target index " + targetIndex);
            standaloneActiveTime = input.readLong();
            lastActiveTimes = new long[numTargets];
            for (int i = 0; i < numTargets; i++)
                lastActiveTimes[i] = input.readLong();
        }

        // Apply data
        this.currentTargetIndex = targetIndex;
        for (int i = 0; i < this.targets.size(); i++) {
            final FallbackTarget target = this.targets.get(i);
            target.lastActiveTime = lastActiveTimes[i] != 0 ? new Date(lastActiveTimes[i]) : null;
        }
        this.lastStandaloneActiveTime = standaloneActiveTime != 0 ? new Date(standaloneActiveTime) : null;
    }

    private void writeStateFile() throws IOException {

        // Sanity check
        assert Thread.holdsLock(this);

        // Write data
        final FileOutputStream fileOutput = !this.isWindows() ?
          new AtomicUpdateFileOutputStream(this.stateFile) : new FileOutputStream(this.stateFile);
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(fileOutput))) {
            output.writeInt(STATE_FILE_COOKIE);
            output.writeInt(CURRENT_FORMAT_VERSION);
            output.writeInt(this.targets.size());
            output.writeInt(this.currentTargetIndex);
            output.writeLong(this.lastStandaloneActiveTime != null ? this.lastStandaloneActiveTime.getTime() : 0);
            for (final FallbackTarget target : this.targets)
                output.writeLong(target.lastActiveTime != null ? target.lastActiveTime.getTime() : 0);
        }
    }

    private boolean isWindows() {
        return System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).contains("win");
    }

// FallbackFuture

    class FallbackFuture extends AbstractFuture<Void> {

        FallbackFuture(ListenableFuture<Void> innerFuture) {
            Futures.addCallback(innerFuture, new FutureCallback<Void>() {
                @Override
                public void onFailure(Throwable t) {
                    FallbackFuture.this.notifyAsync(t);
                }
                @Override
                public void onSuccess(Void value) {
                    FallbackFuture.this.notifyAsync(null);
                }
            }, MoreExecutors.directExecutor());
        }

        @Override
        protected boolean set(Void value) {
            this.forget();
            try {
                return super.set(value);
            } catch (Throwable t2) {
                FallbackKVDatabase.this.log.error("exception from key watch listener", t2);
                return true;
            }
        }

        @Override
        protected boolean setException(Throwable t) {
            this.forget();
            try {
                return super.setException(t);
            } catch (Throwable t2) {
                FallbackKVDatabase.this.log.error("exception from key watch listener", t2);
                return true;
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            this.forget();
            return super.cancel(mayInterruptIfRunning);
        }

        private void notifyAsync(final Throwable t) {
            this.forget();
            final ScheduledExecutorService notifyExecutor;
            synchronized (FallbackKVDatabase.this) {
                notifyExecutor = FallbackKVDatabase.this.executor;
            }
            if (notifyExecutor == null)                             // small shutdown race window here
                return;
            notifyExecutor.submit(() -> this.notify(t));
        }

        private void notify(Throwable t) {
            if (t != null)
                this.setException(t);
            else
                this.set(null);
        }

        private void forget() {
            synchronized (FallbackKVDatabase.this) {
                FallbackKVDatabase.this.futures.remove(this);
            }
        }
    }

// AvailabilityCheckTask

    private class AvailabilityCheckTask implements Runnable {

        private final FallbackTarget target;
        private final int startCount;

        AvailabilityCheckTask(FallbackTarget target) {
            assert Thread.holdsLock(FallbackKVDatabase.this);
            this.target = target;
            this.startCount = FallbackKVDatabase.this.startCount;
        }

        @Override
        public void run() {
            try {
                FallbackKVDatabase.this.performCheck(this.target, this.startCount);
            } catch (Throwable t) {
                FallbackKVDatabase.this.log.error("exception from {} availability check", this.target, t);
            }
        }
    }

// MigrationCheckTask

    private class MigrationCheckTask implements Runnable {

        private final int startCount;

        MigrationCheckTask() {
            assert Thread.holdsLock(FallbackKVDatabase.this);
            this.startCount = FallbackKVDatabase.this.startCount;
        }

        @Override
        public void run() {
            try {
                FallbackKVDatabase.this.checkMigration(this.startCount);
            } catch (Throwable t) {
                FallbackKVDatabase.this.log.error("exception from migration check", t);
            }
        }
    }
}
