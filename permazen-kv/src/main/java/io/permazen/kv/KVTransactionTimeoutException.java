
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

/**
 * Thrown when a {@link KVTransaction} is kept open for too long.
 */
@SuppressWarnings("serial")
public class KVTransactionTimeoutException extends StaleKVTransactionException {

    private static final String DEFAULT_MESSAGE = "transaction cannot be accessed because it has timed out";

    public KVTransactionTimeoutException(KVTransaction kvt) {
        super(kvt, DEFAULT_MESSAGE);
    }

    public KVTransactionTimeoutException(KVTransaction kvt, Throwable cause) {
        super(kvt, DEFAULT_MESSAGE, cause);
    }

    public KVTransactionTimeoutException(KVTransaction kvt, String message) {
        super(kvt, message);
    }

    public KVTransactionTimeoutException(KVTransaction kvt, String message, Throwable cause) {
        super(kvt, message, cause);
    }
}
