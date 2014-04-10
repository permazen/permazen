
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

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
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public TransactionException(Transaction tx, String message) {
        super(message);
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        this.tx = tx;
    }

    /**
     * Get the associated transaction.
     */
    public Transaction getTransaction() {
        return this.tx;
    }
}

