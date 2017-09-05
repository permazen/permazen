
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when a transaction that has been committed, rolled back, or otherwise invalidated is accessed.
 */
@SuppressWarnings("serial")
public class StaleTransactionException extends TransactionException {

    /**
     * Constructor.
     *
     * @param tx the stale transaction
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public StaleTransactionException(Transaction tx) {
        super(tx, "transaction cannot be accessed because it is no longer usable");
    }

    /**
     * Constructor.
     *
     * @param tx the stale transaction
     * @param message exception message
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public StaleTransactionException(Transaction tx, String message) {
        super(tx, message);
    }
}

