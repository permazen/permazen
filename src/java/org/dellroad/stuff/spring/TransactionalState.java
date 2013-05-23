
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Holds transactional state that tracks Spring transactions, following open transactions, commits and roll-backs.
 *
 * <p>
 * The primary methods are {@link #get} and {@link #set set()}. These methods provide a transactional view
 * of their contained state. The transactions follow the Spring transaction in the current thread, if any.
 * For example, if {@link #set set()} is invoked while a Spring transaction is active in the current thread,
 * other threads continue to see the previous value; once the transaction commits, other threads see the
 * new value. If instead the transaction rolls back, the new value is discarded.
 * </p>
 *
 * <p>
 * This class is intended to be used by beans that participate in transactions but are also stateful.
 * Application inconsistency bugs will result when a transaction is rolled-back while the changes made to
 * a stateful bean during that transaction are not. This class provides an automated way to synchronize these
 * transaction events and boundaries.
 * </p>
 *
 * <p>
 * If there are multiple simultaneous transactions, each transaction will see its own copy of the original
 * committed value, and the last transaction's commit will overwrite any previously committed value.
 * </p>
 *
 * <p>
 * Subclasses must implement {@link #deepCopy deepCopy} so that the contained value can be deep-copied for new transactions.
 * </p>
 *
 * <p>
 * Null values are not supported. Nor are nested transactions supported (they are treated like a single transaction).
 * </p>
 *
 * <p>
 * Instances of this class are thread-safe. However, higher-layer classes that utilize this class may
 * require some additional synchronization for their own correctness.
 * </p>
 *
 * @param <T> the type of the contained state
 * @see RetryTransaction
 * @see TransactionSynchronizationManager
 */
public abstract class TransactionalState<T> {

    private final ThreadLocal<T> transactional = new ThreadLocal<T>();
    private T committed;

    /**
     * Constructor.
     *
     * @param initialValue initial committed value
     * @throws IllegalArgumentException if {@code initialValue} is null
     */
    public TransactionalState(T initialValue) {
        if (initialValue == null)
            throw new IllegalArgumentException("null initialValue");
        this.committed = initialValue;
    }

    /**
     * Get the current value.
     *
     * <p>
     * If there is a transaction open in the current thread, this method returns the value associated with
     * the transaction (the "transactional" value); on the first access in a transaction,
     * the {@linkplain #getCommitted committed value} is copied and associated with the transaction.
     * </p>
     *
     * <p>
     * If there is a no transaction open in the current thread, this method returns the
     * {@linkplain #getCommitted committed value}.
     * </p>
     *
     * @return the current transactional value, or the {@linkplain #getCommitted committed value}
     * if no transaction is open in the current thread; in either case, never null
     */
    public T get() {

        // Is there a transaction active in the current thread?
        if (!TransactionSynchronizationManager.isActualTransactionActive())
            return this.getCommitted();

        // Is a transactional value already set?
        T value = this.transactional.get();
        if (value != null)
            return value;

        // Copy the committed value
        synchronized (this) {
            value = this.deepCopy(this.committed);
        }
        if (value == null)
            throw new RuntimeException("internal error: deepCopy() returned a null value");

        // Associate it with the current transaction
        this.transactional.set(value);

        // Regsiter clean-up handler
        this.registerCleanupHandler();

        // Done
        return value;
    }

    /**
     * Get the current committed value. The returned value is the actual value, not a copy.
     * This method ignores any transaction associated with the current thread.
     *
     * @return current committed value, never null
     */
    public synchronized T getCommitted() {
        return this.committed;
    }

    /**
     * Set the current value.
     *
     * <p>
     * This method sets the transactional value if there is a transaction open in the current thread,
     * otherwise it sets the {@linkplain #getCommitted committed value}.
     * </p>
     *
     * @param value new value
     * @throws IllegalArgumentException if {@code value} is null
     */
    public void set(T value) {

        // Sanity check
        if (value == null)
            throw new IllegalArgumentException("null value");

        // If no transaction is associated with the current thread, set committed value
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            this.commit(value);
            return;
        }

        // If no transactional value already set, register adapter
        if (this.transactional.get() == null)
            this.registerCleanupHandler();

        // Associate value with the current transaction
        this.transactional.set(value);
    }

    /**
     * Deep copy the given value.
     *
     * @param value value to copy, never null
     * @return deep copy of {@code value}, never null
     */
    protected abstract T deepCopy(T value);

    private synchronized void commit(T value) {
        if (value == null)
            throw new RuntimeException("internal error: commit() invoked with a null value");
        this.committed = value;
    }

    private void registerCleanupHandler() {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            @Override
            public void afterCompletion(int status) {
                final T value = TransactionalState.this.transactional.get();
                if (status == TransactionSynchronization.STATUS_COMMITTED)
                    TransactionalState.this.commit(value);
                TransactionalState.this.transactional.remove();                                // be gc friendly
            }
        });
    }
}

