
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

/**
 * Exception thrown when an attempt to access a {@link KVTransaction} results
 * in a conflict or other condition requiring that the transaction be retried.
 *
 * <p>
 * <b>Note:</b> although it is usually the case, a {@link RetryKVTransactionException}
 * does <b>not</b> necessarily mean that the transaction failed. In some situations
 * (e.g., involving failed network communication) it may not be possible to determine
 * whether a transaction succeeded or not, and this situation is indicated by throwing a
 * {@link RetryKVTransactionException}. Whether and how often such a "false negative" can
 * occur is a function of the {@link KVDatabase} implementation being used; ideally, all
 * transactions are idempotent and therefore correct in either case.
 */
@SuppressWarnings("serial")
public class RetryKVTransactionException extends KVTransactionException {

    private static final String DEFAULT_MESSAGE = "transaction must be retried";

    public RetryKVTransactionException(KVTransaction kvt) {
        super(kvt, DEFAULT_MESSAGE);
    }

    public RetryKVTransactionException(KVTransaction kvt, Throwable cause) {
        super(kvt, DEFAULT_MESSAGE, cause);
    }

    public RetryKVTransactionException(KVTransaction kvt, String message) {
        super(kvt, message);
    }

    public RetryKVTransactionException(KVTransaction kvt, String message, Throwable cause) {
        super(kvt, message, cause);
    }

    @Override
    public RetryKVTransactionException duplicate() {
        return (RetryKVTransactionException)super.duplicate();
    }
}
