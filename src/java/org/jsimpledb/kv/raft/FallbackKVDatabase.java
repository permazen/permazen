
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition-tolerant {@link KVDatabase} that automatically migrates between a clustered {@link RaftKVDatabase}
 * and a local, non-clustered "emergency standalone mode" {@link KVDatabase}, based on availability of the Raft cluster.
 *
 * <p>
 * A {@link RaftKVDatabase} requires that the local node be part of a cluster majority, otherwise, transactions
 * cannot commit (even read-only ones) and application progress halts. This class adds partition tolerance to
 * a {@link RaftKVDatabase}, by having a local {@link KVDatabase} that can be used when the cluster is unavailable.
 *
 * <p>
 * Instances transparently and automatically switch over to the "standalone mode" {@link KVDatabase} when they determine
 * that the {@link RaftKVDatabase} is unavailable. If/when availability is been restored, the local node rejoins the
 * {@link RaftKVDatabase} cluster. In both cases, a configurable {@link MergeStrategy} is used to migrate the data.
 * In the fallback operation, the most up-to-date committed version of the Raft database is migrated; reading this
 * information does not require communication with the rest of the cluster and therefore is possible during a partition.
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

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Configured state
    private KVDatabase standaloneKV;
    private ArrayList<FallbackTarget> targets;

    // Runtime state
    private boolean migrating;
    private int migrationCount;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> migrationCheckFuture;
    private int startCount;
    private boolean started;
    private int currentTargetIndex;

// Methods

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
     * Configure the underlying {@link FallbackTarget}(s).
     *
     * <p>
     * If multiple {@link FallbackTarget}s are provided, they should be ordered in order of increasing preference.
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
        this.targets = new ArrayList<>(targets.size());
        for (FallbackTarget target : targets) {
            Preconditions.checkArgument(target != null, "null target");
            target = target.clone();
            Preconditions.checkArgument(target.getRaftKVDatabase() != null, "target with no database configured");
        }
    }

// KVDatabase

    @Override
    public synchronized void start() {

        // Already started?
        if (this.started)
            return;
        this.startCount++;

        // Sanity check
        Preconditions.checkState(this.targets != null, "no targets configured");
        try {

            // Create executor
            this.executor = Executors.newScheduledThreadPool(this.targets.size(), new ExecutorThreadFactory());

            // Start underlying databases
            for (FallbackTarget target : this.targets)
                target.getRaftKVDatabase().start();

            // Start periodic checks
            final int currentStartCount = this.startCount;
            for (FallbackTarget target : this.targets) {
                target.future = this.executor.scheduleWithFixedDelay(
                  new AvailabilityCheckTask(target), 0, target.getCheckInterval(), TimeUnit.MILLISECONDS);
            }

            // Initialize target runtime state
            for (FallbackTarget target : this.targets) {
                target.available = true;
                target.lastChangeTimestamp = null;
            }
            this.currentTargetIndex = this.targets.size() - 1;
            this.migrationCount = 0;

            // Set up periodic migration checks
            this.migrationCheckFuture = this.executor.scheduleWithFixedDelay(
              new MigrationCheckTask(), MIGRATION_CHECK_INTERVAL, MIGRATION_CHECK_INTERVAL, TimeUnit.MILLISECONDS);

            // Done
            this.started = true;
        } finally {
            if (!this.started)
                this.cleanup();
        }
    }

    @Override
    public synchronized void stop() {

        // Already stopped?
        if (!this.started)
            return;
        this.cleanup();
    }

    private void cleanup() {

        // Sanity check
        assert Thread.holdsLock(this);

        // Wait for migration to complete
        if (this.migrating) {
            if (this.log.isDebugEnabled())
                this.log.info("waiting for migration to finish to shut down " + this);
            do {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    this.log.warn("interrupted during " + this + " shutdown while waiting for migration to finish (ignoring)", e);
                }
                if (!this.started)              // we lost a race with another thread invoking stop()
                    return;
            } while (this.migrating);
            if (this.log.isDebugEnabled())
                this.log.info("migration finished, continuing with shut down of " + this);
        }

        // Reset target runtime state
        for (FallbackTarget target : this.targets) {
            target.available = false;
            target.lastChangeTimestamp = null;
        }

        // Stop periodic checks
        for (FallbackTarget target : this.targets) {
            if (target.future != null) {
                target.future.cancel(true);
                target.future = null;
            }
        }
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
                this.log.warn("error stopping database target " + target + " (ignoring)", e);
            }
        }

        // Done
        this.started = false;
    }

    @Override
    public synchronized KVTransaction createTransaction() {

        // Sanity check
        Preconditions.checkState(this.started, "not started");

        // Create inner transaction from current database
        final KVDatabase currentKV = this.currentTargetIndex == -1 ?
          this.standaloneKV : this.targets.get(this.currentTargetIndex).getRaftKVDatabase();
        KVTransaction tx = currentKV.createTransaction();

        // Wrap it
        return new FallbackKVTransaction(this, tx, this.migrationCount);
    }

// Package methods

    boolean isMigrating() {
        return this.migrating;
    }

    int getMigrationCount() {
        return this.migrationCount;
    }

// Internal methods

    // Perform availability check on the specified target
    private void performCheck(FallbackTarget target, final int startCount) {

        // Check for shutdown race condition
        synchronized (this) {
            if (!this.started || startCount != this.startCount)
                return;
        }

        // Perform check
        boolean available = false;
        try {
            available = target.checkAvailability();
        } catch (Exception e) {
            if (this.log.isDebugEnabled())
                this.log.debug("checkAvailable() for " + target + " threw exception", e);
        }

        // Handle result
        synchronized (this) {

            // Check for shutdown race condition
            if (!this.started || startCount != this.startCount)
                return;

            // Any state change?
            if (available == target.available)
                return;

            // Enforce hysteresis
            if (target.lastChangeTimestamp != null) {
                final int minDelay = target.available ? target.getMinAvailableTime() : target.getMinUnavailableTime();
                if (target.lastChangeTimestamp.offsetFromNow() + minDelay > 0) {
                    if (this.log.isTraceEnabled()) {
                        this.log.trace(target + " is " + (target.available ? "" : "un") + "available, but minimum delay of "
                          + minDelay + "ms has not yet elapsed");
                    }
                    return;
                }
            }

            // Change state and schedule migration
            this.log.info(target + " has become " + (available ? "" : "un") + "available");
            target.available = available;
            target.lastChangeTimestamp = new Timestamp();
            this.executor.submit(new MigrationCheckTask());
        }
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

            // Allow only one migration at a time
            if (this.migrating)
                return;

            // Get the highest priority (i.e., best choice) database that is currently available
            bestIndex = this.targets.size() - 1;
            while (bestIndex >= 0 && !this.targets.get(bestIndex).available)
                bestIndex--;

            // Already there?
            currIndex = this.currentTargetIndex;
            if (currIndex == bestIndex)
                return;

            // Get targets
            currTarget = currIndex != -1 ? this.targets.get(currIndex) : null;
            bestTarget = bestIndex != -1 ? this.targets.get(bestIndex) : null;

            // Start migration
            this.migrating = true;
        }
        final String desc = "migration from " + currTarget + " (index " + currIndex + ") to "
          + bestTarget + " (index " + bestIndex + ")";
        Throwable error = null;
        try {

            // Gather info
            final KVDatabase currKV = currTarget != null ? currTarget.getRaftKVDatabase() : this.standaloneKV;
            final KVDatabase bestKV = bestTarget != null ? bestTarget.getRaftKVDatabase() : this.standaloneKV;
            final MergeStrategy mergeStrategy = bestIndex < currIndex ?
              currTarget.getUnavailableMergeStrategy() : bestTarget.getRejoinMergeStrategy();

            // Logit
            if (this.log.isDebugEnabled())
                this.log.debug("starting " + desc + " using " + mergeStrategy);

            // Create source transaction. Note the combination of read-only and EVENTUAL_FAST is important, because this
            // guarantees that the transaction will generate no network traffic (and not require any majority) on commit().
            final KVTransaction src;
            if (currKV instanceof RaftKVDatabase) {
                src = ((RaftKVDatabase)currKV).createTransaction(Consistency.EVENTUAL_FAST);
                ((RaftKVTransaction)src).setReadOnly(true);
            } else
                src = currKV.createTransaction();
            boolean srcCommitted = false;
            try {

                // Create destination transaction
                final KVTransaction dst = bestKV.createTransaction();
                boolean dstCommitted = false;
                try {

                    // Perform merge
                    mergeStrategy.merge(src, dst);

                    // Commit transactions
                    src.commit();
                    srcCommitted = true;
                    dst.commit();
                    dstCommitted = true;
                } finally {
                    if (!dstCommitted)
                        dst.rollback();
                }
            } catch (Error e) {
                error = e;
                throw e;
            } catch (RuntimeException e) {
                error = e;
                throw e;
            } catch (Throwable t) {
                error = t;
                throw new RuntimeException(t);
            } finally {
                if (!srcCommitted)
                    src.rollback();
            }
        } finally {

            // Finish up
            synchronized (this) {
                if (error == null) {
                    this.currentTargetIndex = bestIndex;
                    this.migrationCount++;
                }
                this.migrating = false;
                this.notifyAll();
            }

            // Logit
            if (error != null)
                this.log.error(desc + " failed", error);
            else if (this.log.isDebugEnabled())
                this.log.debug(desc + " succeeded");
        }
    }

// ExecutorThreadFactory

    private class ExecutorThreadFactory implements ThreadFactory {

        private final AtomicInteger id = new AtomicInteger();

        @Override
        public Thread newThread(Runnable action) {
            final Thread thread = new Thread(action);
            thread.setName("Executor#" + this.id.incrementAndGet() + " for " + FallbackKVDatabase.this);
            return thread;
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
                FallbackKVDatabase.this.log.error("exception from " + this.target + " availability check", t);
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

