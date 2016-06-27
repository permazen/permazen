
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.fallback;

import com.google.common.base.Preconditions;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;

import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.Timestamp;
import org.jsimpledb.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents one of the underlying {@link RaftKVDatabase}s managed by a {@link FallbackKVDatabase}.
 *
 * <p>
 * Instances must at least be configured with the associated {@link RaftKVDatabase}; all other properties have defaults.
 *
 * Two {@link MergeStrategy}s are configured to handle switching into
 * (see {@link #setUnavailableMergeStrategy setUnavailableMergeStrategy()}) and out of
 * (see {@link #setRejoinMergeStrategy setRejoinMergeStrategy()}) standalone mode.
 *
 * <p>
 * Other parameters configure how cluster availability is determined and enforce hysteresis; see
 * {@link #setTransactionTimeout setTransactionTimeout()}, {@link #setCheckInterval setCheckInterval()},
 * {@link #setMinAvailableTime setMinAvailableTime()}, and {@link #setMinUnavailableTime setMinUnavailableTime()}.
 */
public class FallbackTarget implements Cloneable {

    /**
     * Default transaction timeout for assessing availability ({@value #DEFAULT_TRANSACTION_TIMEOUT}ms).
     *
     * @see #setTransactionTimeout setTransactionTimeout()
     */
    public static final int DEFAULT_TRANSACTION_TIMEOUT = 1000;

    /**
     * Default check interval ({@value #DEFAULT_CHECK_INTERVAL}ms).
     *
     * @see #setCheckInterval setCheckInterval()
     */
    public static final int DEFAULT_CHECK_INTERVAL = 2000;

    /**
     * Default minimum available type ({@value #DEFAULT_MIN_AVAILABLE_TIME}ms).
     *
     * @see #setMinAvailableTime setMinAvailableTime()
     */
    public static final int DEFAULT_MIN_AVAILABLE_TIME = 10 * 1000;

    /**
     * Default minimum unavailable type ({@value #DEFAULT_MIN_UNAVAILABLE_TIME}ms).
     *
     * @see #setMinUnavailableTime setMinUnavailableTime()
     */
    public static final int DEFAULT_MIN_UNAVAILABLE_TIME = 30 * 1000;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Runtime state
    boolean available;
    Date lastActiveTime;
    Timestamp lastChangeTimestamp;
    ScheduledFuture<?> future;

    // Configuration state
    private RaftKVDatabase raft;
    private int transactionTimeout = DEFAULT_TRANSACTION_TIMEOUT;
    private int checkInterval = DEFAULT_CHECK_INTERVAL;
    private int minAvailableTime = DEFAULT_MIN_AVAILABLE_TIME;
    private int minUnavailableTime = DEFAULT_MIN_UNAVAILABLE_TIME;
    private MergeStrategy unavailableMergeStrategy = new OverwriteMergeStrategy();
    private MergeStrategy rejoinMergeStrategy = new NullMergeStrategy();

// Configuration State

    /**
     * Get the {@link RaftKVDatabase}.
     *
     * @return underlying database
     */
    public RaftKVDatabase getRaftKVDatabase() {
        return this.raft;
    }

    /**
     * Set the {@link RaftKVDatabase}.
     *
     * @param raft underlying database
     */
    public void setRaftKVDatabase(RaftKVDatabase raft) {
        this.raft = raft;
    }

    /**
     * Get the transaction timeout used when determining database availability.
     *
     * @return transaction timeout in milliseconds
     */
    public int getTransactionTimeout() {
        return this.transactionTimeout;
    }

    /**
     * Configure the transaction timeout used when determining database availability.
     *
     * <p>
     * Default is {@link #DEFAULT_TRANSACTION_TIMEOUT}.
     *
     * @param timeout timeout in milliseconds
     * @throws IllegalArgumentException if {@code timeout} is not greater than zero
     */
    public void setTransactionTimeout(int timeout) {
        Preconditions.checkArgument(timeout > 0, "timeout <= 0");
        this.transactionTimeout = timeout;
    }

    /**
     * Get the interval between availability checks.
     *
     * @return check interval in milliseconds
     */
    public int getCheckInterval() {
        return this.checkInterval;
    }

    /**
     * Configure the interval between availability checks.
     *
     * <p>
     * Default is {@link #DEFAULT_CHECK_INTERVAL}.
     *
     * @param checkInterval check interval in milliseconds
     * @throws IllegalArgumentException if {@code checkInterval} is not greater than zero
     */
    public void setCheckInterval(int checkInterval) {
        Preconditions.checkArgument(checkInterval > 0, "checkInterval <= 0");
        this.checkInterval = checkInterval;
    }

    /**
     * Get the minimum amount of time after becoming available before allowing this instance to become unavailable again.
     *
     * @return minimum available time in milliseconds
     */
    public int getMinAvailableTime() {
        return this.minAvailableTime;
    }

    /**
     * Configure the minimum amount of time after becoming available before allowing this instance to become unavailable again.
     *
     * <p>
     * Default is {@link #DEFAULT_MIN_AVAILABLE_TIME}.
     *
     * @param minAvailableTime minimum available time in milliseconds
     * @throws IllegalArgumentException if {@code minAvailableTime} is not greater than zero
     */
    public void setMinAvailableTime(int minAvailableTime) {
        Preconditions.checkArgument(minAvailableTime > 0, "minAvailableTime <= 0");
        this.minAvailableTime = minAvailableTime;
    }

    /**
     * Get the minimum amount of time after becoming unavailable before allowing this instance to become available again.
     *
     * @return minimum unavailable time in milliseconds
     */
    public int getMinUnavailableTime() {
        return this.minUnavailableTime;
    }

    /**
     * Configure the minimum amount of time after becoming unavailable before allowing this instance to become available again.
     *
     * <p>
     * Default is {@link #DEFAULT_MIN_UNAVAILABLE_TIME}.
     *
     * @param minUnavailableTime minimum unavailable time in milliseconds
     * @throws IllegalArgumentException if {@code minUnavailableTime} is not greater than zero
     */
    public void setMinUnavailableTime(int minUnavailableTime) {
        Preconditions.checkArgument(minUnavailableTime > 0, "minUnavailableTime <= 0");
        this.minUnavailableTime = minUnavailableTime;
    }

    /**
     * Get the merge strategy to apply when transitioning from this database to a lower priority database
     * because our {@link RaftKVDatabase} cluster has become unavailable.
     *
     * @return unavailable merge strategy
     */
    public MergeStrategy getUnavailableMergeStrategy() {
        return this.unavailableMergeStrategy;
    }

    /**
     * Configure the merge strategy to apply when transitioning from this database to a lower priority database
     * because our {@link RaftKVDatabase} cluster has become unavailable.
     *
     * <p>
     * Default is {@link OverwriteMergeStrategy}.
     *
     * @param strategy unavailable merge strategy
     * @throws IllegalArgumentException if {@code strategy} is null
     */
    public void setUnavailableMergeStrategy(MergeStrategy strategy) {
        Preconditions.checkArgument(strategy != null, "null strategy");
        this.unavailableMergeStrategy = strategy;
    }

    /**
     * Get the merge strategy to apply when transitioning from a lower priority database to this database
     * because our {@link RaftKVDatabase} cluster has become available.
     *
     * @return rejoin merge strategy
     */
    public MergeStrategy getRejoinMergeStrategy() {
        return this.rejoinMergeStrategy;
    }

    /**
     * Configure the merge strategy to apply when transitioning from a lower priority database to this database
     * because our {@link RaftKVDatabase} cluster has become available again.
     *
     * <p>
     * Default is {@link NullMergeStrategy}.
     *
     * @param strategy rejoin merge strategy
     * @throws IllegalArgumentException if {@code strategy} is null
     */
    public void setRejoinMergeStrategy(MergeStrategy strategy) {
        Preconditions.checkArgument(strategy != null, "null strategy");
        this.rejoinMergeStrategy = strategy;
    }

// Runtime State

    /**
     * Get the current availability of this target.
     *
     * @return true if this target is currently available, otherwise false
     */
    public boolean isAvailable() {
        return this.available;
    }

    /**
     * Get the time of the last change in availability of this target, if known.
     *
     * @return time this target's availability last changed, or null if unknown or no change has occurred
     */
    public Date getLastChangeTime() {
        return this.lastChangeTimestamp != null ?
          new Date(System.currentTimeMillis() + this.lastChangeTimestamp.offsetFromNow()) : null;
    }

    /**
     * Get the last time this target was the active database.
     *
     * @return last active time of this target, or null if never active
     */
    public Date getLastActiveTime() {
        return this.lastActiveTime;
    }

// Subclass Methods

    /**
     * Perform an availability assessment of the underlying {@link RaftKVDatabase} associated with this instance.
     *
     * <p>
     * This method may block for as long as necessary to determine availability, but it should not block
     * indefinitely. If a {@link KVTransaction} is opened, {@link KVTransaction#setTimeout setTimeout()}
     * should be used to set a time limit.
     *
     * <p>
     * This method will not be invoked concurrently by two different threads.
     *
     * <p>
     * If this method throws an unchecked exception, the database will be assumed to be unavailable.
     *
     * <p>
     * The implementation in {@link FallbackTarget} determines availability by attempting to commit a read-only,
     * {@link org.jsimpledb.kv.raft.Consistency#LINEARIZABLE} transaction within the configured maximum timeout.
     *
     * @return true if database is available, false otherwise
     */
    protected boolean checkAvailability() {

        // Setup
        if (this.log.isTraceEnabled())
            this.log.trace("checking availability of " + this.raft + " with timeout of " + this.transactionTimeout + "ms");
        final long startTimeNanos = System.nanoTime();
        boolean success = false;

        // Perform transaction
        final KVTransaction tx = this.raft.createTransaction();
        try {
            tx.setTimeout(this.transactionTimeout);
            tx.getAtLeast(ByteUtil.EMPTY);
            tx.commit();
            success = true;
        } finally {
            if (!success)
                tx.rollback();
        }

        // Check timeout
        final int duration = (int)((System.nanoTime() - startTimeNanos) / 1000000L);
        final boolean timedOut = duration > this.transactionTimeout;
        if (this.log.isTraceEnabled()) {
            this.log.trace("availability transaction on " + this.raft + " completed in "
              + duration + "ms (" + (timedOut ? "failed" : "successful") + ")");
        }
        if (timedOut && this.log.isDebugEnabled()) {
            this.log.debug("availability transaction on " + this.raft + " completed in "
              + duration + " > " + this.transactionTimeout + " ms, returning unavailable");
        }

        // Done
        return !timedOut;
    }

// Cloneable

    public FallbackTarget clone() {
        try {
            return (FallbackTarget)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[raft=" + this.raft + "]";
    }
}

