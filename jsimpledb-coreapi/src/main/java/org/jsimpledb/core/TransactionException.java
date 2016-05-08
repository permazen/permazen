
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;

/**
 * Superclass of exceptions associated with a specific {@link Transaction}.
 */
@SuppressWarnings("serial")
public class TransactionException extends DatabaseException {

    private final Transaction tx;

    /**
     * Constructor.
     *
     * @param tx the transaction
     * @param message exception message
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public TransactionException(Transaction tx, String message) {
        super(message);
        Preconditions.checkArgument(tx != null, "null tx");
        this.tx = tx;
    }

    /**
     * Get the associated transaction.
     *
     * @return the transaction in which the error occurred
     */
    public Transaction getTransaction() {
        return this.tx;
    }
}

