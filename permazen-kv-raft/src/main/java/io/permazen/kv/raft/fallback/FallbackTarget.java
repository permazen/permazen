
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.fallback;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVTransaction;
import io.permazen.kv.raft.Consistency;
import io.permazen.kv.raft.Follower;
import io.permazen.kv.raft.LeaderRole;
import io.permazen.kv.raft.RaftKVDatabase;
import io.permazen.kv.raft.Role;
import io.permazen.kv.raft.Timestamp;
import io.permazen.util.ByteData;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

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

    private static final int MAX_2NODE_FOLLOWER_STALENESS = DEFAULT_TRANSACTION_TIMEOUT * 2;

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
        final Date copy = this.lastActiveTime;
        return copy != null ? (Date)copy.clone() : null;
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
     * {@link Consistency#LINEARIZABLE} transaction within the configured maximum timeout.
     *
     * @param fallbackDB parent fallback database
     * @return true if database is available, false otherwise
     */
    protected boolean checkAvailability(FallbackKVDatabase fallbackDB) {
        Preconditions.checkArgument(fallbackDB != null, "null fallbackDB");

        // Check whether we're even configured first - a read-only tx is allowed when unconfigured
        if (!this.raft.isConfigured()) {
            if (this.log.isTraceEnabled())
                this.log.trace("checking availability of {} - cluster is not configured", this.raft);
            return false;
        }

        // Setup
        if (this.log.isTraceEnabled())
            this.log.trace("checking availability of {} with timeout of {}ms", this.raft, this.transactionTimeout);

        // See if a linearizable transaction has committed recently, otherwise perform one ourselves
        final Timestamp linearizableCommitTimestamp = this.raft.getLinearizableCommitTimestamp();
        if (linearizableCommitTimestamp != null && linearizableCommitTimestamp.offsetFromNow() >= -this.checkInterval) {
            if (this.log.isTraceEnabled()) {
                this.log.trace("a linearizable tx on {} completed {}ms ago, so we'll consider that evidence of availability",
                  this.raft, linearizableCommitTimestamp.offsetFromNow());
            }
        } else {

            // Perform transaction
            final long startTimeNanos = System.nanoTime();
            boolean success = false;
            final KVTransaction tx = fallbackDB.createAvailabilityCheckTransaction(this.raft);
            try {
                tx.setTimeout(this.transactionTimeout);
                tx.get(ByteData.empty());
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
                this.log.trace("availability transaction on {} completed in {}ms ({})",
                  this.raft, duration, timedOut ? "failed" : "successful");
            }
            if (timedOut) {
                if (this.log.isDebugEnabled()) {
                    this.log.trace("availability transaction on {} completed in {} > {} ms, returning unavailable",
                      this.raft, duration, this.transactionTimeout);
                }
                return false;
            }
        }

        // If we're the leader of a two node cluster, read-only TX's on a broken network will continue to work until a read-write
        // transaction happens, because any new leader would require our vote, so we know we're still leader. Detect this case
        // and fail the check if so.
        final Role role = this.raft.getCurrentRole();
        if (role instanceof LeaderRole) {
            final List<Follower> followerList = ((LeaderRole)role).getFollowers();
            if (followerList.size() == 1) {
                final Follower follower = followerList.get(0);
                final Timestamp leaderTimestamp = follower.getLeaderTimestamp();
                if (leaderTimestamp == null || -leaderTimestamp.offsetFromNow() > MAX_2NODE_FOLLOWER_STALENESS) {
                    if (this.log.isDebugEnabled()) {
                        this.log.debug("single follower's leader timestamp is {} > {} ms stale, returning unavailable",
                          leaderTimestamp != null ? -leaderTimestamp.offsetFromNow() : null, MAX_2NODE_FOLLOWER_STALENESS);
                    }
                    return false;
                }
            }
        }

        // Done
        return true;
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
