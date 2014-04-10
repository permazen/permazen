
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

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
}

