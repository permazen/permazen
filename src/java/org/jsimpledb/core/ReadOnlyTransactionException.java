
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Thrown by a {@link Transaction} that has been {@linkplain Transaction#setReadOnly set read-only}
 * when any mutating operation is attempted.
 */
@SuppressWarnings("serial")
public class ReadOnlyTransactionException extends TransactionException {

    /**
     * Constructor.
     *
     * @param tx the read-only transaction
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public ReadOnlyTransactionException(Transaction tx) {
        super(tx, "transaction is read-only");
    }
}

