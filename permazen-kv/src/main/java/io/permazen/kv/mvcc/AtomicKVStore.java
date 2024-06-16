
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;

/**
 * Extension of the {@link KVStore} interface for implementations that support atomic, batched reads and writes.
 *
 * <p>
 * Atomic batch reads are available via {@link #readOnlySnapshot}, which returns a consistent point-in-time views of the
 * {@link KVStore}. Atomic batch writes are available via {@link #apply(Mutations, boolean) apply()}, which applies a set
 * of mutations in an "all or none" fashion.
 */
public interface AtomicKVStore extends KVStore {

// Lifecycle

    /**
     * Start this instance. This method must be called prior to creating any transactions.
     *
     * <p>
     * This method is idempotent: if this instance is already started, nothing happens.
     *
     * <p>
     * Whether an instance that has been started and stopped can be restarted is implementation-dependent.
     *
     * @throws IllegalStateException if this instance is not properly configured
     */
    void start();

    /**
     * Stop this instance.
     *
     * <p>
     * Any open {@link #readOnlySnapshot}'s should be {@link CloseableKVStore#close close()}'d before invoking this method;
     * the behavior of those that are not is undefined.
     *
     * <p>
     * This method is idempotent: if this instance has not been started, or is already stopped, nothing happens.
     */
    void stop();

// Access

    /**
     * Create a read-only "snapshot" view of this instance equal to its current state.
     *
     * <p>
     * The returned {@link CloseableKVStore} should be treated as read-only. It may not actually be read-only,
     * but if it's not, then any changes should have no effect on this instance. The returned {@link CloseableKVStore}
     * must be completely independent from this instance (subsequent changes to either one do not affect the other).
     *
     * <p>
     * The returned {@link CloseableKVStore} should be promply {@link CloseableKVStore#close close()}'d when no longer
     * needed to release any underlying resources.
     *
     * @return read-only, snapshot view of this instance
     * @throws io.permazen.kv.StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws io.permazen.kv.RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    CloseableKVStore readOnlySnapshot();

    /**
     * Apply a set of mutations to this instance atomically.
     *
     * <p>
     * If this method returns normally, all of the given mutations will have been applied. If this method
     * returns abnormally, then none of the given mutations will have been applied.
     *
     * <p>
     * In any case, other threads observing this instance will never see a partial application of the given mutations.
     *
     * <p>
     * This method is required to apply the mutations in this order: removes, puts, adjusts.
     *
     * <p>
     * If {@code sync} is true, the implementation must durably persist the changes before returning.
     *
     * @param mutations the mutations to apply
     * @param sync if true, caller requires that the changes be durably persisted
     * @throws io.permazen.kv.StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws io.permazen.kv.RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws UnsupportedOperationException if {@code sync} is true and this implementation cannot guarantee durability
     * @throws IllegalArgumentException if {@code mutations} is null
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    void apply(Mutations mutations, boolean sync);

    /**
     * Apply all the given {@link Mutations} to this instance.
     *
     * <p>
     * Equivalent to: {@link #apply(Mutations, boolean) apply}{@code (mutations, false)}.
     *
     * @param mutations mutations to apply
     */
    @Override
    default void apply(Mutations mutations) {
        this.apply(mutations, false);
    }
}
