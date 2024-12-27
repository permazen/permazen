
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;

import java.util.concurrent.Future;

/**
 * {@link KVDatabase} transaction API.
 *
 * <p>
 * Provides a transactional view of a {@link KVStore}.
 *
 * <p>
 * Instances may throw {@link KVTransactionException} during any operation if the transaction cannot be continued.
 * In particular, {@link StaleKVTransactionException} is thrown by a transaction that is no longer open, and
 * {@link RetryKVTransactionException} is thrown when the transaction should be retried due to a transient
 * problem (such as a write conflict with another transaction).
 *
 * <p>
 * When {@link RetryKVTransactionException} is thrown by {@link #commit}, the transaction may have actually been committed.
 * Therefore, transactions should be written to be idempotent.
 *
 * <p>
 * No matter what state it is in, instances must support invoking {@link #rollback} at any time.
 *
 * <p>
 * If an instance throws a {@link KVTransactionException}, the transaction should be implicitly rolled back.
 * Any subsequent operation other than {@link #rollback} should throw {@link StaleKVTransactionException}.
 *
 * <p>
 * Except for {@link #rollback} and methods that just query status, implementations must throw {@link StaleKVTransactionException}
 * if {@link #commit} or {@link #rollback} has already been invoked, or if the {@link KVTransaction} instance is no longer usable
 * for some other reason. In particular, implementations should throw {@link KVTransactionTimeoutException} if an operation
 * is attempted on a transaction that has been held open past some maximum allowed time limit.
 *
 * <p>
 * Implementations are not required to support accessing keys that start with {@code 0xff},
 * and if not may throw {@link IllegalArgumentException} if such keys are accessed.
 *
 * <p>
 * Note: for some implementations, the data read from a transaction that is never {@link #commit}'ed is
 * not guaranteed to be up to date.
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
     * @throws StaleKVTransactionException if this transaction is no longer usable
     */
    void setTimeout(long timeout);

    /**
     * Determine whether this transaction is read-only.
     *
     * <p>
     * Default is false.
     *
     * @return true if this instance is read-only
     * @throws StaleKVTransactionException if this transaction is no longer usable
     */
    boolean isReadOnly();

    /**
     * Enable or disable read-only mode.
     *
     * <p>
     * Read-only transactions allow mutations, but all changes are discarded on {@link #commit}.
     *
     * <p>
     * Some implementations may impose one or more of the following restrictions on this method:
     * <ul>
     *  <li>{@link #setReadOnly setReadOnly()} may only be invoked prior to accessing data;</li>
     *  <li>{@link #setReadOnly setReadOnly()} may only be invoked prior to mutating data; and/or</li>
     *  <li>Once set to read-only, a transaction may not be set back to read-write</li>
     * </ul>
     * If one of the above constraints is violated, an {@link IllegalStateException} is thrown.
     *
     * <p>
     * Note: for some implementations, the data read from a transaction that is never {@link #commit}'ed is
     * not guaranteed to be up to date, even if that transaction is read-only.
     *
     * <p>
     * Default is false.
     *
     * @param readOnly read-only setting
     * @throws IllegalStateException if the implementation doesn't support changing read-only status at this time
     * @throws StaleKVTransactionException if this transaction is no longer usable
     */
    void setReadOnly(boolean readOnly);

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
     * single key that is used to consolidate modifications to some set of keys; at the Permazen layer, modification
     * to multiple objects and/or fields can detected and consolidated using an
     * {@link io.permazen.annotation.OnChange &#64;OnChange} method that increments a single {@link io.permazen.Counter}
     * field, whose key is then watched (to determine the key corresponding to a Java model object field, use
     * {@link io.permazen.PermazenField#getKey(io.permazen.PermazenObject) PermazenField.getKey()}).
     *
     * <p>
     * Conceptually, detection of changes behaves as if by a background thread that periodically creates a new transaction
     * and reads the key's value (the actual implementation will likely be more efficient). This means a change that is
     * quickly reverted could be missed, and that multiple changes could occur before notification. In addition, spurious
     * notifications may occur, where the key's value has not changed.
     *
     * <p>
     * A key watch is only guaranteed to be valid if the transaction in which it was created successfully commits.
     * In particular, nothing is specified about how or whether {@link Future}s associated with failed transactions complete,
     * so the {@link Future}s returned by this method should not be relied on until after a successful commit (perhaps with
     * the help of a {@linkplain io.permazen.core.Transaction#addCallback transaction callback}).
     *
     * <p>
     * Key watch support is optional; instances that don't support key watches throw {@link UnsupportedOperationException}.
     * Some implementations may only support watching a key that already exists.
     *
     * <p>
     * Note: many {@link KVDatabase} implementations actually return a
     * {@link com.google.common.util.concurrent.ListenableFuture}. However, listeners must not perform any
     * long running or blocking operations. Also, because the semantics of {@link RetryKVTransactionException} allow for
     * the possibility that the transaction actually did commit, "duplicate" listener notifications could occur.
     *
     * <p>
     * Key watch {@link Future}s that have not completed yet, but are no longer needed, must be {@link Future#cancel cancel()}'ed
     * to avoid memory leaks.
     *
     * <p>
     * Key watch support is indepdendent of whether the transaction is {@linkplain #setReadOnly read-only}.
     *
     * @param key the key to watch
     * @return a {@link Future} that returns {@code key} when the value associated with {@code key} is modified
     * @throws StaleKVTransactionException if this transaction is no longer usable
     * @throws RetryKVTransactionException if this transaction must be retried and is no longer usable
     * @throws KVDatabaseException if an unexpected error occurs
     * @throws UnsupportedOperationException if this instance does not support key watches
     * @throws IllegalArgumentException if {@code key} starts with {@code 0xff} and such keys are not supported
     * @throws IllegalArgumentException if {@code key} is null
     * @see io.permazen.PermazenTransaction#getKey(io.permazen.PermazenObject) PermazenTransaction.getKey()
     * @see io.permazen.PermazenField#getKey(io.permazen.PermazenObject) PermazenField.getKey()
     */
    Future<Void> watchKey(ByteData key);

    /**
     * Commit this transaction.
     *
     * <p>
     * Note that if this method throws a {@link RetryKVTransactionException},
     * the transaction was either successfully committed or rolled back. In either case,
     * this instance is no longer usable.
     *
     * <p>
     * Note also for some implementations, even read-only transactions must be {@link #commit}'ed in order for the
     * data accessed during the transaction to be guaranteed to be up to date.
     *
     * @throws StaleKVTransactionException if this transaction is no longer usable
     * @throws RetryKVTransactionException if this transaction must be retried and is no longer usable
     */
    void commit();

    /**
     * Cancel this transaction, if not already canceled.
     *
     * <p>
     * After this method returns, this instance is no longer usable.
     *
     * <p>
     * Note: for some implementations, rolling back a transaction invalidates guarantees about the the data read
     * during the transaction being up to date, even if the transaction was {@link #setReadOnly setReadOnly()}.
     *
     * <p>
     * This method may be invoked at any time, even after a previous invocation of
     * {@link #commit} or {@link #rollback}, in which case the invocation will be ignored.
     * In particular, this method must <b>not</b> throw {@link StaleKVTransactionException}.
     */
    void rollback();

    /**
     * Apply weaker transaction consistency while performing the given action, if supported.
     *
     * <p>
     * Some implementations support reads with weaker consistency guarantees. These reads generate fewer transaction
     * conflicts but return possibly out-of-date information. Depending on the implementation, when operating in this
     * mode writes may not be supported and may generate an {@link IllegalStateException} or just be ignored.
     *
     * <p>
     * The weaker consistency is only applied for the current thread, and it ends when this method returns.
     *
     * <p>
     * <b>This method is for experts only</b>; inappropriate use can result in a corrupted database.
     * You should not make any changes to the database after this method returns based on any information
     * read by the {@code action}.
     *
     * <p>
     * The implementation in {@link KVTransaction} just performs {@code action} normally.
     *
     * @param action the action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    default void withWeakConsistency(Runnable action) {
        Preconditions.checkArgument(action != null, "null action");
        action.run();
    }

    /**
     * Create a read-only snapshot of the database content represented by this transaction.
     *
     * <p>
     * The returned {@link CloseableKVStore} should be treated as read-only. It may not actually be read-only,
     * but if it's not, then any changes should have no effect on this instance. The returned {@link CloseableKVStore}
     * must be completely independent from this transaction (subsequent changes to either one do not affect the other).
     *
     * <p>
     * Note: as with any other information extracted from a {@link KVTransaction}, the returned content should not be
     * considered valid until this transaction has been successfully committed.
     *
     * <p>
     * The returned {@link CloseableKVStore} should be promply {@link CloseableKVStore#close close()}'d when no longer
     * needed to release any underlying resources. In particular, the caller must ensure that the {@link CloseableKVStore}
     * is {@link CloseableKVStore#close close()}'d even if this transaction's commit fails. This may require
     * adding a transaction synchronization callback, etc.
     *
     * <p>
     * This is an optional method; only some underlying key/value store technologies can efficiently support it.
     * Implementations should throw {@link UnsupportedOperationException} if not supported.
     *
     * @return independent, read-only copy of this transaction's entire database content
     * @throws UnsupportedOperationException if this method is not supported
     * @throws StaleKVTransactionException if this transaction is no longer usable
     * @throws RetryKVTransactionException if this transaction must be retried and is no longer usable
     */
    CloseableKVStore readOnlySnapshot();
}
