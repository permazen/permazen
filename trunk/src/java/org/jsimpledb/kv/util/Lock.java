
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.Comparator;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.util.ByteUtil;

/**
 * Read/write lock of a {@link KeyRange}.
 * Instances are immutable.
 *
 * @see LockManager
 */
class Lock extends KeyRange {

    /**
     * Sorts locks by min value, then read locks before write locks.
     * Note: the comparison does not include the {@linkplain #getOwner owner} in the comparison.
     */
    public static final Comparator<Lock> MIN_COMPARATOR = new Comparator<Lock>() {
        @Override
        public int compare(Lock lock1, Lock lock2) {
            int diff = KeyRange.compare(lock1.min, KeyRange.MIN, lock2.min, KeyRange.MIN);
            if (diff != 0)
                return diff;
            diff = Boolean.compare(lock1.write, lock2.write);
            if (diff != 0)
                return diff;
            return 0;
        }
    };

    /**
     * Sorts locks by max value, then read locks before write locks.
     * Note: the comparison does not include the {@linkplain #getOwner owner} in the comparison.
     */
    public static final Comparator<Lock> MAX_COMPARATOR = new Comparator<Lock>() {
        @Override
        public int compare(Lock lock1, Lock lock2) {
            int diff = KeyRange.compare(lock1.max, KeyRange.MAX, lock2.max, KeyRange.MAX);
            if (diff != 0)
                return diff;
            diff = Boolean.compare(lock1.write, lock2.write);
            if (diff != 0)
                return diff;
            return 0;
        }
    };

    private static final LockOwner DUMMY_OWNER = new LockOwner();

    final boolean write;
    final LockOwner owner;

    /**
     * Constructor. The current thread becomes the implicit {@linkplain #getOwner owner} of the lock.
     *
     * @param min min key, or null for no minimum
     * @param max max key, or null for no maximum
     * @param write true for write lock, false for read lock
     * @throws IllegalArgumentException if {@code owner} is null
     * @throws IllegalArgumentException if {@code min > max}
     */
    public Lock(LockOwner owner, byte[] min, byte[] max, boolean write) {
        super(min, max);
        if (owner == null)
            throw new IllegalArgumentException("null owner");
        this.owner = owner;
        this.write = write;
    }

    /**
     * Get the owner of this lock.
     */
    public LockOwner getOwner() {
        return this.owner;
    }

    /**
     * Get whether this is a read lock or a write lock.
     *
     * @return true if this lock is a write lock, false if this lock is a read lock
     */
    public boolean isWrite() {
        return this.write;
    }

    /**
     * Determine if this lock and another lock conflict.
     *
     * <p>
     * Two locks conflict if:
     * <ul>
     *  <li>The locks {@linkplain KeyRange#overlaps overlap}</li>
     *  <li>Either lock is a {@linkplain #isWrite write lock}</li>
     *  <li>The two locks have different {@linkplain #getOwner owners}</li>
     * </ul>
     * </p>
     *
     * @param that other lock
     * @return true if this lock and {@code that} conflict
     */
    public boolean conflictsWith(Lock that) {
        return this.overlaps(that) && (this.write || that.write) && !this.owner.equals(that.owner);
    }

    /**
     * Determine if this lock and the given lock can be combined into a single lock,
     * and return the combined lock if so. Locks should be merged when possible for efficiency
     * (the semantics of what is locked does not change).
     *
     * <p>
     * Two locks can be merged if:
     * <ul>
     *  <li>The locks have the same {@linkplain #getOwner owner}</li>
     *  <li>The union of the two key ranges results in a single contiguous range</li>
     *  <li>If one lock is a read lock and the other is a write lock,
     *      the write lock {@linkplain KeyRange#contains contains} the read lock</li>
     * </ul>
     * </p>
     *
     * @param that lock to merge with this one
     * @return combined lock, or null if this lock is not mergable with {@code that}
     */
    public Lock mergeWith(Lock that) {

        // Must have same owner
        if (!this.owner.equals(that.owner))
            return null;

        // Union must be contiguous (i.e., the intervals must overlap or "touch" adjacently)
        if (Lock.compare(this.min, KeyRange.MIN, that.max, KeyRange.MAX) > 0
          || Lock.compare(that.min, KeyRange.MIN, this.max, KeyRange.MAX) > 0)
            return null;

        // If not the same r/w type, the write lock must contain the read lock, otherwise we could conflict with a co-reader
        if (this.write != that.write) {
            if (!(this.write ? this.contains(that) : that.contains(this)))
                return null;
        }

        // Merge locks
        final byte[] newMin = Lock.compare(this.min, KeyRange.MIN, that.min, KeyRange.MIN) < 0 ? this.min : that.min;
        final byte[] newMax = Lock.compare(this.max, KeyRange.MAX, that.max, KeyRange.MAX) > 0 ? this.max : that.max;
        return new Lock(this.owner, newMin, newMax, this.write || that.write);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final Lock that = (Lock)obj;
        return this.owner.equals(that.owner) && this.write == that.write;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.owner.hashCode() ^ (this.write ? 1 : 0);
    }

    @Override
    public String toString() {
        return "Lock[owner=" + this.owner + ",min=" + ByteUtil.toString(this.min) + ",max="
          + ByteUtil.toString(this.max) + ",type=" + (this.write ? "write" : "read") + "]";
    }

    /**
     * Get a search key for use with instances sorted via {@link #MIN_COMPARATOR}.
     */
    public static Lock getMinKey(byte[] min, boolean write) {
        if (min == null)
            throw new IllegalArgumentException("null min");
        return new Lock(DUMMY_OWNER, min, min, write);
    }

    /**
     * Get a search key for use with instances sorted via {@link #MAX_COMPARATOR}.
     */
    public static Lock getMaxKey(byte[] max, boolean write) {
        if (max == null)
            throw new IllegalArgumentException("null max");
        return new Lock(DUMMY_OWNER, max, max, write);
    }
}

