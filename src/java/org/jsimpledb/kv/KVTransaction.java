
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

/**
 * {@link KVDatabase} transaction API.
 *
 * <p>
 * Provides a transactional view into a {@link KVStore}.
 * </p>
 *
 * <p>
 * Instances may throw {@link KVTransactionException} during any operation if the transaction cannot be continued.
 * In particular, {@link StaleTransactionException} is thrown by a transaction that is no longer open, and
 * {@link RetryTransactionException} is thrown when the transaction should be retried due to a transient
 * problem (such as a write conflict with another transaction). When {@link RetryTransactionException} is thrown by
 * {@link #commit}, the transaction may have actually been committed. Therefore, transactions should be written to be idempotent.
 * When any {@link KVTransactionException} is thrown, the transaction must still support invoking {@link #rollback},
 * but all other operations may throw {@link StaleTransactionException}.
 * </p>
 *
 * <p>
 * If an instance throws a {@link KVTransactionException}, the transaction should be implicitly rolled back.
 * </p>
 *
 * <p>
 * Implementations must throw {@link StaleTransactionException} if {@link #commit} or {@link #rollback} has already
 * been invoked, or if the {@link KVTransaction} instance is no longer usable for some other reason. In particular,
 * implementations should throw {@link TransactionTimeoutException} if an operation is attempted on a transaction
 * that has been held open past some maximum allowed time limit.
 * </p>
 *
 * <p>
 * Implementations are responsible for ensuring modifications to {@code byte[]} arrays after method
 * invocations do no harm. This usually means {@code byte[]} array parameters and return values must be copied.
 * </p>
 *
 * <p>
 * Accessing keys that start with {@code 0xff} is not supported and will result in {@link IllegalArgumentException}.
 * </p>
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
     * Cancel this transaction.
     *
     * <p>
     * After this method returns, this instance is no longer usable.
     *
     * <p>
     * This method may be invoked at any time, even after a previous invocation of
     * {@link #commit} or {@link #rollback}, in which case the invocation will be ignored.
     */
    void rollback();
}

