
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

/**
 * Extension of the {@link KVStore} interface for implementations that support atomic, batched reads and writes.
 *
 * <p>
 * Atomic batch reads are available via {@link #snapshot}, which returns a consistent point-in-time views of the {@link KVStore}.
 * Atomic batch writes are available via {@link #mutate mutate()}, which applies a set of mutations in an "all or none" fashion.
 */
public interface AtomicKVStore extends KVStore {

    /**
     * Acquire a read-only, snapshot view of this instance based on the current state.
     *
     * <p>
     * The returned {@link KVStore} view should remain constant even if this instance is subsequently mutated.
     *
     * <p>
     * Some implementations may return a {@link KVStore} that also implements {@link AutoCloseable}; callers are
     * encouraged to detect and {@link java.io.Closeable#close} such instances when no longer needed.
     *
     * @return read-only, snapshot view of this instance
     * @throws StaleTransactionException if an underlying transaction is no longer usable
     * @throws RetryTransactionException if an underlying transaction must be retried and is no longer usable
     */
    KVStore snapshot();

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
     * If {@code sync} is true, the implementation must durably persist the changes before returning.
     *
     * @param removes {@link #removeRange removeRange} operations to apply, or null for none
     * @param puts {@link #put put()} operations to apply, or null for none
     * @param sync if true, caller requires that the changes be durably persisted
     * @throws StaleTransactionException if an underlying transaction is no longer usable
     * @throws RetryTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws UnsupportedOperationException if {@code sync} is true and this implementation cannot guarantee durability
     * @throws NullPointerException if an iterator returns a null element
     */
    void mutate(Iterable<? extends KeyRange> removes, Iterable<? extends KVPair> puts, boolean sync);
}

