
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.KVStore;

/**
 * Presents a mutable view of an underlying read-only {@link KVStore} and records the mutations in memory.
 *
 * <p>
 * Instances intercept all operations to the underlying {@link KVStore}, recording mutations in a {@link Writes} instance
 * instead of applying them to the {@link KVStore}. Instances provide a view of the mutated {@link KVStore} based those
 * mutations which is always up-to-date, i.e., mutations that overwrite previous mutations are consolidated, etc.
 *
 * <p>
 * The resulting {@link Writes} represent the delta between the underlying {@link KVStore} and this {@link KVStore}.
 * This delta can be applied later to another {@link KVStore} via {@link KVStore#apply KVStore.apply()}.
 *
 * <p>
 * Reads are passed through to the underlying {@link KVStore} except where they intersect a previous write.
 *
 * <p>
 * Instances ensure that counter adjustment mutations are atomic, so they never overlap put or remove mutations.
 *
 * <p>
 * In all cases, the underlying {@link KVStore} is never modified.
 *
 * <p>
 * <b>Read Tracking</b>
 *
 * <p>
 * Instances may optionally be configured to track and record all keys read in a {@link Reads} object. When reads are being
 * tracked, tracking may be temporarily suspended in the current thread via {@link #withoutReadTracking withoutReadTracking()}.
 * Read tracking may also be permanently disabled (and any recorded reads discarded) via {@link #disableReadTracking}.
 *
 * <p>
 * <b>Thread Safety</b>
 *
 * <p>
 * Instances are thread safe and always present an up-to-date view even in the face of multiple threads making changes.
 * However, directly accessing the associated {@link Reads} or {@link Writes} is not thread safe without first locking
 * the {@link DeltaKVStore} that owns them.
 */
public interface DeltaKVStore extends KVStore {

    /**
     * Get the {@link KVStore} that underlies this instance.
     *
     * <p>
     * Note that in some implementations the returned object and/or its contents may change over time, for example,
     * if this instance gets "rebased" on a newer underlying {@link KVStore}.
     *
     * @return underlying {@link KVStore}
     */
    KVStore getBaseKVStore();

    /**
     * Get the {@link Reads} associated with this instance.
     *
     * <p>
     * This includes all keys explicitly or implicitly read by calls to
     * {@link #get get()}, {@link #getAtLeast getAtLeast()}, {@link #getAtMost getAtMost()}, and {@link #getRange getRange()}.
     *
     * <p>
     * The returned object is "live" and should only be accessed while synchronized on this instance.
     *
     * <p>
     * The read tracking may be imprecise, as long as all actual reads are included. For example, if keys {@code 10001},
     * {@code 100002}, and {@code 100003} were read, the returned {@link Reads} may contain those three keys, or it may
     * contain the entire range {@code 10001-10003}, even though some keys in that range were not actually read in order
     * to save memory. This optimization is acceptable as long as the keys that were actually read are always included.
     *
     * @return reads recorded, or null if this instance is not configured to record reads or read tracking has
     *  been permanently disabled via {@link #disableReadTracking}
     */
    Reads getReads();

    /**
     * Get the {@link Writes} associated with this instance.
     *
     * <p>
     * The returned object is "live" and should only be accessed while synchronized on this instance.
     *
     * @return writes recorded
     */
    Writes getWrites();

    /**
     * Determine if this instance is read-only.
     *
     * @return true if this instance is read-only, otherwise false
     */
    boolean isReadOnly();

    /**
     * Configure this instance as read-only.
     *
     * <p>
     * Any subsequent write attempts will result in an {@link IllegalStateException}.
     *
     * <p>
     * This operation cannot be un-done.
     */
    void setReadOnly();

// ReadTracking

    /**
     * Permanently disable read tracking and discard the {@link Reads} associated with this instance.
     *
     * <p>
     * This can be used to save some memory when read tracking information is no longer needed.
     *
     * <p>
     * Does nothing if read tracking is already disabled.
     */
    void disableReadTracking();

    /**
     * Temporarily disable read tracking in the current thread while performing the given action.
     *
     * <p>
     * If {@code allowWrites} is false, then any write attempts by {@code action} will provoke an
     * {@link IllegalStateException}.
     *
     * @param allowWrites whether to allow writes
     * @param action the action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    void withoutReadTracking(boolean allowWrites, Runnable action);
}
