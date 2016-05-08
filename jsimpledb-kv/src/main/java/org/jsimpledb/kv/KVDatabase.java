
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import java.util.Map;

/**
 * A transactional database with a simple key/value API.
 *
 * @see KVTransaction
 */
public interface KVDatabase {

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
     * This method is idempotent: if this instance has not been started, or is already stopped, nothing happens.
     */
    void stop();

// Transactions

    /**
     * Create a new transaction.
     *
     * @return newly created transaction
     * @throws KVDatabaseException if an unexpected error occurs
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    KVTransaction createTransaction();

    /**
     * Create a new transaction with the specified options.
     *
     * @param options optional transaction options; may be null
     * @return newly created transaction
     * @throws KVDatabaseException if an unexpected error occurs
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    KVTransaction createTransaction(Map<String, ?> options);
}

