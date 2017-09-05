
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

/**
 * Superclass of all unchecked exceptions thrown by a {@link ArrayKVStore}.
 */
@SuppressWarnings("serial")
public class ArrayKVException extends RuntimeException {

    public ArrayKVException() {
    }

    public ArrayKVException(String message) {
        super(message);
    }

    public ArrayKVException(Throwable cause) {
        super(cause);
    }

    public ArrayKVException(String message, Throwable cause) {
        super(message, cause);
    }
}

