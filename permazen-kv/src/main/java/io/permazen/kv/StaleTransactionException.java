
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

/**
 * Thrown when an operation is attempted on a {@link KVTransaction} that is no longer usable.
 */
@SuppressWarnings("serial")
public class StaleTransactionException extends KVTransactionException {

    private static final String DEFAULT_MESSAGE = "transaction cannot be accessed because it is no longer usable";

    public StaleTransactionException(KVTransaction kvt) {
        super(kvt, DEFAULT_MESSAGE);
    }

    public StaleTransactionException(KVTransaction kvt, Throwable cause) {
        super(kvt, DEFAULT_MESSAGE, cause);
    }

    public StaleTransactionException(KVTransaction kvt, String message) {
        super(kvt, message);
    }

    public StaleTransactionException(KVTransaction kvt, String message, Throwable cause) {
        super(kvt, message, cause);
    }

    @Override
    public StaleTransactionException duplicate() {
        return (StaleTransactionException)super.duplicate();
    }
}

