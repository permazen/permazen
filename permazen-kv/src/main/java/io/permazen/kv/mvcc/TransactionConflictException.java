
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVTransaction;
import io.permazen.kv.RetryTransactionException;

/**
 * Exception thrown when a transaction fails because of an MVCC conflict.
 */
@SuppressWarnings("serial")
public class TransactionConflictException extends RetryTransactionException {

    private final Conflict conflict;

    public TransactionConflictException(KVTransaction kvt, Conflict conflict) {
        this(kvt, conflict, String.valueOf(conflict));
    }

    public TransactionConflictException(KVTransaction kvt, Conflict conflict, String message) {
        super(kvt, message);
        Preconditions.checkArgument(conflict != null, "null conflict");
        this.conflict = conflict;
    }

    /**
     * Get the conflict that generated this exception.
     *
     * @return the conflict causing this exception
     */
    public Conflict getConflict() {
        return this.conflict;
    }

    @Override
    public TransactionConflictException duplicate() {
        return (TransactionConflictException)super.duplicate();
    }
}
