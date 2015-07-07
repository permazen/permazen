
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mvcc;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVStore;

/**
 * Extension of the {@link KVStore} interface for implementations that support atomic, batched reads and writes.
 *
 * <p>
 * Atomic batch reads are available via {@link #snapshot}, which returns a consistent point-in-time views of the {@link KVStore}.
 * Atomic batch writes are available via {@link #mutate mutate()}, which applies a set of mutations in an "all or none" fashion.
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
     * Any open {@link #snapshot}'s should be {@link CloseableKVStore#close close()}'d before invoking this method;
     * the behavior of those that are not is undefined.
     *
     * <p>
     * This method is idempotent: if this instance has not been started, or is already stopped, nothing happens.
     */
    void stop();

// Access

    /**
     * Acquire a read-only, snapshot view of this instance based on the current state.
     *
     * <p>
     * The returned {@link KVStore} view should remain constant even if this instance is subsequently mutated.
     *
     * <p>
     * Note: callers are required to {@link CloseableKVStore#close close} the returned instance when no longer in use.
     *
     * @return read-only, snapshot view of this instance
     * @throws org.jsimpledb.kv.StaleTransactionException if an underlying transaction is no longer usable
     * @throws org.jsimpledb.kv.RetryTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    CloseableKVStore snapshot();

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
     * @throws org.jsimpledb.kv.StaleTransactionException if an underlying transaction is no longer usable
     * @throws org.jsimpledb.kv.RetryTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws UnsupportedOperationException if {@code sync} is true and this implementation cannot guarantee durability
     * @throws IllegalArgumentException if {@code mutations} is null
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    void mutate(Mutations mutations, boolean sync);
}

