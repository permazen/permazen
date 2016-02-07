
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import java.util.concurrent.Future;

/**
 * {@link KVDatabase} transaction API.
 *
 * <p>
 * Provides a transactional view of a {@link KVStore}.
 *
 * <p>
 * Instances may throw {@link KVTransactionException} during any operation if the transaction cannot be continued.
 * In particular, {@link StaleTransactionException} is thrown by a transaction that is no longer open, and
 * {@link RetryTransactionException} is thrown when the transaction should be retried due to a transient
 * problem (such as a write conflict with another transaction).
 *
 * <p>
 * When {@link RetryTransactionException} is thrown by {@link #commit}, the transaction may have actually been committed.
 * Therefore, transactions should be written to be idempotent.
 *
 * <p>
 * No matter what state it is in, instances must support invoking {@link #rollback} at any time.
 *
 * <p>
 * If an instance throws a {@link KVTransactionException}, the transaction should be implicitly rolled back.
 * Any subsequent operation other than {@link #rollback} should throw {@link StaleTransactionException}.
 *
 * <p>
 * Implementations must throw {@link StaleTransactionException} if {@link #commit} or {@link #rollback} has already
 * been invoked, or if the {@link KVTransaction} instance is no longer usable for some other reason. In particular,
 * implementations should throw {@link TransactionTimeoutException} if an operation is attempted on a transaction
 * that has been held open past some maximum allowed time limit.
 *
 * <p>
 * Implementations are responsible for ensuring modifications to {@code byte[]} arrays after method
 * invocations do no harm. This usually means {@code byte[]} array parameters and return values must be copied.
 *
 * <p>
 * Implementations are not required to support accessing keys that start with {@code 0xff},
 * and if not may throw {@link IllegalArgumentException} if such keys are accessed.
 */
public interface KVTransaction extends KVStore {

    /**
     * Get the {@link KVDatabase} with which this instance is associated.
     *
     * @return associated database
     */
    KVDatabase getKVDatabase();

    /**
     * Change the timeout for this transaction from its default value (optional operation).
     *
     * @param timeout transaction timeout in milliseconds, or zero for unlimited
     * @throws UnsupportedOperationException if this transaction does not support timeouts
     * @throws IllegalArgumentException if {@code timeout} is negative
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    void setTimeout(long timeout);

    /**
     * Watch a key to monitor for changes in its value.
     *
     * <p>
     * When this method is invoked, {@code key}'s current value (if any) as read by this transaction is remembered. The returned
     * {@link Future} completes if and when a different value for {@code key} is subsequently committed by some transaction,
     * including possibly this one. This includes creation or deletion of the key.
     *
     * <p>
     * Key watches outlive the transaction in which they are created, persisting until they complete or are
     * {@link Future#cancel cancel()}'ed. When a {@link KVDatabase} is {@link KVDatabase#stop}'ed, all outstanding
     * key watches are implicitly {@link Future#cancel cancel()}'ed.
     *
     * <p><b>Caveats</b></p>
     *
     * <p>
     * Key watches are not without overhead; applications should avoid overuse. For example, consider creating a
     * single key that is used to consolidate modifications to a set of keys; at the JSimpleDB layer, modification
     * of multiple objects and/or fields could detected by an {@link org.jsimpledb.annotation.OnChange &#64;OnChange}
     * method that increments a single {@link org.jsimpledb.Counter} whose key is then watched.
     *
     * <p>
     * Conceptually, detection of changes behaves as if by a background thread that periodically creates a new transaction
     * and reads the key's value (the actual implementation will likely be more efficient). This means a change that is
     * quickly reverted could be missed, and that multiple changes could occur before notification. In addition, spurious
     * notifications may occur, where the key's value has not changed.
     *
     * <p>
     * A key watch is only guaranteed to be valid if the transaction in which it was created successfully commits.
     *
     * <p>
     * Key watch support is optional; instances that don't support key watches throw {@link UnsupportedOperationException}.
     *
     * <p>
     * Note: many {@link KVDatabase} implementations actually return a
     * {@link com.google.common.util.concurrent.ListenableFuture}.
     *
     * @param key the key to watch
     * @return a {@link Future} that returns {@code key} when the value associated with {@code key} is modified
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws RetryTransactionException if this transaction must be retried and is no longer usable
     * @throws KVDatabaseException if an unexpected error occurs
     * @throws UnsupportedOperationException if this instance does not support key watches
     * @throws IllegalArgumentException if {@code key} starts with {@code 0xff} and such keys are not supported
     * @throws IllegalArgumentException if {@code key} is null
     * @see org.jsimpledb.JTransaction#getKey(org.jsimpledb.JObject, String) JTransaction.getKey()
     */
    Future<Void> watchKey(byte[] key);

    /**
     * Commit this transaction.
     *
     * <p>
     * Note that if this method throws a {@link RetryTransactionException},
     * the transaction was either successfully committed or rolled back. In either case,
     * this instance is no longer usable.
     * </p>
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws RetryTransactionException if this transaction must be retried and is no longer usable
     */
    void commit();

    /**
     * Cancel this transaction, if not already canceled.
     *
     * <p>
     * After this method returns, this instance is no longer usable.
     *
     * <p>
     * This method may be invoked at any time, even after a previous invocation of
     * {@link #commit} or {@link #rollback}, in which case the invocation will be ignored.
     * In particular, this method should <b>not</b> throw {@link StaleTransactionException}.
     */
    void rollback();

    /**
     * Create a mutable copy of the database content represented by this transaction.
     *
     * <p>
     * The returned {@link KVStore} should be mutable, and be completely independent of this transaction
     * (subsequent changes to this transaction do not affect it, and vice-versa).
     *
     * <p>
     * Note that as with any other information extracted from a {@link KVTransaction}, the returned content
     * should not be considered valid until this transaction has been successfully committed.
     *
     * <p>
     * The returned {@link KVStore} should be promply {@link CloseableKVStore#close close()}'d when no longer
     * needed to release any underlying resources. In particular, the caller must ensure that the {@link KVStore}
     * is {@link CloseableKVStore#close close()}'d even if this transaction's commit fails. This may require
     * adding a transaction synchronization callback, etc.
     *
     * <p>
     * This is an optional method; only some underlying key/value store technologies can efficiently support it.
     * Implementations may throw {@link UnsupportedOperationException} if not supported.
     *
     * @return independent, mutable copy of this transaction's entire database content
     * @throws UnsupportedOperationException if this method is not supported
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws RetryTransactionException if this transaction must be retried and is no longer usable
     */
    CloseableKVStore mutableSnapshot();
}

