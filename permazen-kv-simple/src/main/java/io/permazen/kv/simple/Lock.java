
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KeyRange;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.Comparator;

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
    public static final Comparator<Lock> MIN_COMPARATOR = Comparator
      .comparing(Lock::getMin, KeyRange::compare)
      .thenComparing(Lock::isWrite, Boolean::compare);

    /**
     * Sorts locks by max value, then read locks before write locks.
     * Note: the comparison does not include the {@linkplain #getOwner owner} in the comparison.
     */
    public static final Comparator<Lock> MAX_COMPARATOR = Comparator
      .comparing(Lock::getMax, KeyRange::compare)
      .thenComparing(Lock::isWrite, Boolean::compare);

    private static final LockOwner DUMMY_OWNER = new LockOwner();

    final boolean write;
    final LockOwner owner;

    /**
     * Constructor. The current thread becomes the implicit {@linkplain #getOwner owner} of the lock.
     *
     * @param min min key; must not be null
     * @param max max key, or null for no maximum
     * @param write true for write lock, false for read lock
     * @throws IllegalArgumentException if {@code min} is null
     * @throws IllegalArgumentException if {@code owner} is null
     * @throws IllegalArgumentException if {@code min > max}
     */
    Lock(LockOwner owner, ByteData min, ByteData max, boolean write) {
        super(min, max);
        Preconditions.checkArgument(owner != null, "null owner");
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
     *
     * @param that lock to merge with this one
     * @return combined lock, or null if this lock is not mergable with {@code that}
     */
    public Lock mergeWith(Lock that) {

        // Must have same owner
        if (!this.owner.equals(that.owner))
            return null;

        // Union must be contiguous (i.e., the intervals must overlap or "touch" adjacently)
        if (KeyRange.compare(this.min, that.max) > 0 || KeyRange.compare(that.min, this.max) > 0)
            return null;

        // If not the same r/w type, the write lock must contain the read lock, otherwise we could conflict with a co-reader
        if (this.write != that.write) {
            if (!(this.write ? this.contains(that) : that.contains(this)))
                return null;
        }

        // Merge locks
        final ByteData newMin = KeyRange.compare(this.min, that.min) < 0 ? this.min : that.min;
        final ByteData newMax = KeyRange.compare(this.max, that.max) > 0 ? this.max : that.max;
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
    public static Lock getMinKey(ByteData min, boolean write) {
        Preconditions.checkArgument(min != null, "null min");
        return new Lock(DUMMY_OWNER, min, min, write);
    }

    /**
     * Get a search key for use with instances sorted via {@link #MAX_COMPARATOR}.
     */
    public static Lock getMaxKey(ByteData max, boolean write) {
        Preconditions.checkArgument(max != null, "null max");
        return new Lock(DUMMY_OWNER, max, max, write);
    }
}
