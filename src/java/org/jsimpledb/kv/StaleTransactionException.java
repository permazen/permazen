
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

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
}

