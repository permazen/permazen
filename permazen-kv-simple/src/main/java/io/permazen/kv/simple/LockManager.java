
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KeyRanges;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.dellroad.stuff.java.Predicate;
import org.dellroad.stuff.java.TimedWait;

/**
 * Manager of read/write locks on {@code byte[]} key ranges that ensures isolation and serialization while allowing concurrent
 * access by multiple threads to a single underlying {@code byte[]} key/value store.
 *
 * <p>
 * This implementation is straightforward: read locks can overlap, but write locks may not, and all locks owned
 * by the same owner remain in force until all are {@linkplain #release released} at the same time.
 *
 * <p>
 * Instances are configured with a monitor object which is used for all internal locking and inter-thread wait/notify
 * handshaking (by default, this instance). A user-supplied monitor object may be provided via the constructor.
 *
 * <p>
 * Two timeout values are supported:
 * <ul>
 *  <li>The wait timeout (specified as a parameter to {@link #lock lock()}) limits how long a thread will wait
 *      on a lock held by another thread before giving up</li>
 *  <li>The {@linkplain #getHoldTimeout hold timeout} limits how long a thread may hold
 *      on to a contested lock before being forced to release all its locks; after that, the
 *      next call to {@link #lock lock} or {@link #release release} will fail</li>
 * </ul>
 *
 * <p>
 * Note that if the hold timeout is set to zero (unlimited), then an application bug that leaks locks will result
 * in those locks never being released.
 */
public class LockManager {

    private static final long TEN_YEARS_MILLIS = 10L * 365L * 24L * 60L * 60L * 1000L;

    private final Object lockObject;

    // Contains each owner's lock time, or null if hold timeout has expired.
    // Invariant: if owner has any locks, then owner is a key in this map and value is not null.
    // In the case owner's hold timeout expired and another owner forced it to release all of its locks, the expired
    // owner will own no locks but still exist in this map (with a null value), until its next lock() or release().
    private final HashMap<LockOwner, Long> lockTimes = new HashMap<>();

    private final TreeSet<Lock> locksByMin = new TreeSet<>(Lock.MIN_COMPARATOR);            // locks ordered by minimum
    private final TreeSet<Lock> locksByMax = new TreeSet<>(Lock.MAX_COMPARATOR);            // locks ordered by maximum
    private final long nanoBasis = System.nanoTime();

    private long holdTimeout;

    /**
     * Convenience constructor. Equivalent to <code>LockManager(null)</code>.
     */
    public LockManager() {
        this(null);
    }

    /**
     * Primary constructor.
     *
     * @param lockObject Java object used to synchronize field access and inter-thread wait/notify handshake,
     *  or null to use this instance
     */
    public LockManager(Object lockObject) {
        this.lockObject = lockObject != null ? lockObject : this;
    }

    /**
     * Get the hold timeout configured for this instance.
     *
     * <p>
     * The hold timeout limits how long a thread may hold on to a contested lock before being forced to release
     * all of its locks; after that, the next call to {@link #lock lock} or {@link #release release} will fail.
     *
     * @return hold timeout in milliseconds
     */
    public long getHoldTimeout() {
        synchronized (this.lockObject) {
            return this.holdTimeout;
        }
    }

    /**
     * Set the hold timeout for this instance. Default is zero (unlimited).
     *
     * @param holdTimeout how long a thread may hold a contested lock before {@link LockResult#HOLD_TIMEOUT_EXPIRED}
     *  will be returned by {@link #lock lock()} or {@link #release release()} in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code holdTimeout} is negative
     */
    public void setHoldTimeout(long holdTimeout) {
        Preconditions.checkArgument(holdTimeout >= 0, "holdTimeout < 0");
        synchronized (this.lockObject) {
            this.holdTimeout = Math.min(holdTimeout, TEN_YEARS_MILLIS);             // limit to 10 years to avoid overflow
        }
    }

    /**
     * Acquire a lock on behalf of the specified owner.
     *
     * <p>
     * This method will block for up to {@code waitTimeout} milliseconds if the lock is held by
     * another thread, after which point {@link LockResult#WAIT_TIMEOUT_EXPIRED} is returned.
     * The configured locking object will be used for inter-thread wait/notify handshaking.
     *
     * <p>
     * If {@code owner} already holds one or more locks, but the {@linkplain #getHoldTimeout hold timeout} has expired,
     * then {@link LockResult#HOLD_TIMEOUT_EXPIRED} is returned and all of the other locks are will have already been
     * automatically released.
     *
     * <p>
     * Once a lock is successfully acquired, it stays acquired until all locks are released together via {@link #release release()}.
     *
     * @param owner lock owner
     * @param minKey minimum key (inclusive); must not be null
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @param write true for a write lock, false for a read lock
     * @param waitTimeout how long to wait before returning {@link LockResult#WAIT_TIMEOUT_EXPIRED}
     *  in milliseconds, or zero for unlimited
     * @return a {@link LockResult}
     * @throws InterruptedException if the current thread is interrupted while waiting for the lock
     * @throws IllegalArgumentException if {@code owner}, {@code minKey}, or {@code range} is null
     * @throws IllegalArgumentException if {@code minKey > maxKey}
     * @throws IllegalArgumentException if {@code waitTimeout} is negative
     */
    public LockResult lock(LockOwner owner, ByteData minKey, ByteData maxKey, boolean write, long waitTimeout)
      throws InterruptedException {
        synchronized (this.lockObject) {

            // Sanity check
            Preconditions.checkArgument(owner != null, "null owner");
            Preconditions.checkArgument(waitTimeout >= 0, "waitTimeout < 0");
            waitTimeout = Math.min(waitTimeout, TEN_YEARS_MILLIS);                  // limit to 10 years to avoid overflow

            // Check hold timeout
            final long lockerRemaining = this.checkHoldTimeout(owner);
            if (lockerRemaining == -1)
                return LockResult.HOLD_TIMEOUT_EXPIRED;

            // Create lock
            Lock lock = new Lock(owner, minKey, maxKey, write);

            // Wait for lockability, until the first one of:
            //  - Wait timeout
            //  - Locker's hold timeout
            //  - Lock owner's hold timeout
            final LockChecker lockChecker = new LockChecker(lock);
            if (!lockChecker.test()) {
                long timeToWait = Math.min(waitTimeout, lockChecker.getTimeRemaining());
                if (lockerRemaining != 0)
                    timeToWait = Math.min(timeToWait, lockerRemaining);
                if (!TimedWait.wait(this.lockObject, timeToWait, lockChecker))
                    return LockResult.WAIT_TIMEOUT_EXPIRED;
            }

            // Check hold timeout again
            if (this.checkHoldTimeout(owner) == -1)
                return LockResult.HOLD_TIMEOUT_EXPIRED;

            // Merge the lock with other locks it can merge with, removing those locks in the process
            for (Lock that : lockChecker.getMergers()) {
                final Lock mergedLock = lock.mergeWith(that);
                if (mergedLock != null) {
                    this.locksByMin.remove(that);
                    this.locksByMax.remove(that);
                    owner.locks.remove(that);
                    lock = mergedLock;
                }
            }

            // Add lock
            this.locksByMin.add(lock);
            this.locksByMax.add(lock);
            owner.locks.add(lock);

            // Set hold timeout (if not already set)
            if (!this.lockTimes.containsKey(owner)) {
                final long currentTime = System.nanoTime() - this.nanoBasis;
                this.lockTimes.put(owner, currentTime);
            } else
                assert this.lockTimes.get(owner) != null;

            // Done
            return LockResult.SUCCESS;
        }
    }

    /**
     * Determine if the given lock owner holds a lock on the specified range.
     *
     * @param owner lock owner
     * @param minKey minimum key (inclusive); must not be null
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @param write if range must be write locked; if false, may be either read or write locked
     * @return true if the range is locked for writes by {@code owner}
     */
    public boolean isLocked(LockOwner owner, ByteData minKey, ByteData maxKey, boolean write) {
        synchronized (this.lockObject) {
            Preconditions.checkArgument(owner != null, "null owner");
            KeyRanges ranges = new KeyRanges(minKey, maxKey);
            for (Lock lock : owner.locks) {
                if (write && !lock.write)
                    continue;
                ranges.remove(lock);
            }
            return ranges.isEmpty();
        }
    }

    /**
     * Release all locks held by the specified owner.
     *
     * <p>
     * If the owner's {@linkplain #getHoldTimeout hold timeout} has already expired, then all locks will have
     * already been released and false is returned.
     *
     * <p>
     * Does nothing (and returns true) if {@code owner} does not own any locks.
     *
     * @param owner lock owner
     * @return true if successful, false if {@code owner}'s hold timeout expired
     * @throws IllegalArgumentException if {@code owner} is null
     */
    public boolean release(LockOwner owner) {
        Preconditions.checkArgument(owner != null, "null owner");
        synchronized (this.lockObject) {

            // Check if hold timeout has already expired; in any case, remove lock time
            if (this.lockTimes.containsKey(owner)) {
                final Long lockTime = this.lockTimes.remove(owner);
                if (lockTime == null)
                    return false;
            }

            // Release all locks
            this.doRelease(owner);

            // Done
            return true;
        }
    }

    /**
     * Check whether the {@linkplain #getHoldTimeout hold timeout} has expired for the given lock owner
     * and if not return the amount of time remaining.
     *
     * <p>
     * If the owner's hold timeout has expired, then {@code -1} is returned and any locks previously held by {@code owner}
     * will have been automatically released.
     *
     * @param owner lock owner
     * @return milliseconds until {@code owner}'s hold timeout expires, zero if {@code owner} has no hold timeout
     *  (e.g., nothing is locked or hold timeout disabled), or -1 if {@code owner}'s hold timeout has expired
     * @throws IllegalArgumentException if {@code owner} is null
     */
    public long checkHoldTimeout(LockOwner owner) {
        synchronized (this.lockObject) {
            if (this.holdTimeout == 0)
                return 0;
            if (!this.lockTimes.containsKey(owner))
                return 0;
            final Long lockTime = this.lockTimes.get(owner);
            if (lockTime == null)
                return -1;
            final long currentTime = System.nanoTime() - this.nanoBasis;
            final long holdDeadline = lockTime + this.holdTimeout * 1000000L;
            final long remaining = holdDeadline - currentTime;
            if (remaining <= 0) {
                this.lockTimes.put(owner, null);
                this.doRelease(owner);
                return -1;
            }
            return (remaining + 999999L) / 1000000L;
        }
    }

    // Release all locks held by owner. Assumes synchronized already on this.lockObject.
    private void doRelease(LockOwner owner) {
        for (Lock lock : owner.locks) {
            this.locksByMin.remove(lock);
            this.locksByMax.remove(lock);
        }
        owner.locks.clear();
        this.lockObject.notifyAll();
    }

    // Check whether we can lock, and fill the list of mergers and return zero if so. If not, return time remaining.
    // Assumes synchronized already on this.lockObject.
    private long checkLock(Lock lock, List<Lock> mergers) {

        // Get lock's min & max
        final ByteData lockMin = lock.getMin();
        final ByteData lockMax = lock.getMax();

    startOver:
        while (true) {

            // Get locks whose min is < lockMax
            final NavigableSet<Lock> lhs = lockMax == null ? this.locksByMin :
              this.locksByMin.headSet(Lock.getMinKey(lockMax, false), false);

            // Get locks whose max is > lockMin
            final NavigableSet<Lock> rhs = this.locksByMax.tailSet(Lock.getMaxKey(ByteUtil.getNextKey(lockMin), false), true);

            // Find overlapping locks and check for conflicts
            final HashSet<Lock> overlaps = new HashSet<>();
            for (Lock other : lhs) {

                // Does this lock overlap?
                if (!rhs.contains(other))
                    continue;

                // Do this lock & other lock conflict?
                if (lock.conflictsWith(other)) {

                    // See if other lock's owner's hold timeout has expired
                    assert this.lockTimes.containsKey(other.owner);
                    final long remaining = this.checkHoldTimeout(other.owner);
                    if (remaining == -1)
                        continue startOver;

                    // Return time remaining until conflicting owner's hold timeout
                    return Math.max(remaining, 1);
                }

                // Add overlap
                overlaps.add(other);
            }

            //System.out.println(Thread.currentThread().getName() + ": LockChecker: BEFORE: lock = " + lock + "\n"
            //+ "  lockByMin = " + this.locksByMin + "\n"
            //+ "  lockByMax = " + this.locksByMax + "\n"
            //+ "   overlaps = " + overlaps + "\n");

            // Find overlaps we can merge with
            overlaps.stream().filter(other -> lock.mergeWith(other) != null).iterator().forEachRemaining(mergers::add);

            //System.out.println(Thread.currentThread().getName() + ": LockChecker: AFTER: lock = " + lock + "\n"
            //+ "  lockByMin = " + this.locksByMin + "\n"
            //+ "  lockByMax = " + this.locksByMax + "\n"
            //+ "    mergers = " + mergers + "\n");

            // Done
            return 0;
        }
    }

// LockResult

    /**
     * Possible return values from {@link LockManager#lock LockManager.lock()}.
     */
    public enum LockResult {

        /**
         * The lock was successfully acquired.
         */
        SUCCESS,

        /**
         * The timeout expired while waiting to acquire the lock.
         */
        WAIT_TIMEOUT_EXPIRED,

        /**
         * The owner's hold timeout expired.
         */
        HOLD_TIMEOUT_EXPIRED;
    }

// LockChecker predicate

    private class LockChecker implements Predicate {

        private final Lock lock;
        private final ArrayList<Lock> mergers = new ArrayList<>();

        private long timeRemaining;

        LockChecker(Lock lock) {
            this.lock = lock;
        }

        public List<Lock> getMergers() {
            return this.mergers;
        }

        public long getTimeRemaining() {
            return this.timeRemaining;
        }

        @Override
        public boolean test() {

            // Reset state
            this.mergers.clear();

            // See if we can lock
            this.timeRemaining = LockManager.this.checkLock(this.lock, this.mergers);
            return this.timeRemaining == 0;
        }
    }
}
