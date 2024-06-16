
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

/**
 * Thrown when an operation is attempted on a {@link KVTransaction} that is no longer usable.
 */
@SuppressWarnings("serial")
public class StaleKVTransactionException extends KVTransactionException {

    private static final String DEFAULT_MESSAGE = "transaction cannot be accessed because it is no longer usable";

    public StaleKVTransactionException(KVTransaction kvt) {
        super(kvt, DEFAULT_MESSAGE);
    }

    public StaleKVTransactionException(KVTransaction kvt, Throwable cause) {
        super(kvt, DEFAULT_MESSAGE, cause);
    }

    public StaleKVTransactionException(KVTransaction kvt, String message) {
        super(kvt, message);
    }

    public StaleKVTransactionException(KVTransaction kvt, String message, Throwable cause) {
        super(kvt, message, cause);
    }

    @Override
    public StaleKVTransactionException duplicate() {
        return (StaleKVTransactionException)super.duplicate();
    }
}
