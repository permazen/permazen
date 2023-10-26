
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown on an attempt to {@link Transaction#commit commit()} a transaction that has been marked rollback-only.
 */
@SuppressWarnings("serial")
public class RollbackOnlyTransactionException extends TransactionException {

    /**
     * Constructor.
     *
     * @param tx the transaction
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public RollbackOnlyTransactionException(Transaction tx) {
        super(tx, "transaction has been marked rollback only");
    }
}

