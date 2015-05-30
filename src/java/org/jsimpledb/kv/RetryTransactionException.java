
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

/**
 * Exception thrown when an attempt to access a {@link KVTransaction} results
 * in a conflict requiring that the transaction be restarted.
 */
@SuppressWarnings("serial")
public class RetryTransactionException extends KVTransactionException {

    private static final String DEFAULT_MESSAGE = "transaction must be retried";

    public RetryTransactionException(KVTransaction kvt) {
        super(kvt, DEFAULT_MESSAGE);
    }

    public RetryTransactionException(KVTransaction kvt, Throwable cause) {
        super(kvt, DEFAULT_MESSAGE, cause);
    }

    public RetryTransactionException(KVTransaction kvt, String message) {
        super(kvt, message);
    }

    public RetryTransactionException(KVTransaction kvt, String message, Throwable cause) {
        super(kvt, message, cause);
    }
}

