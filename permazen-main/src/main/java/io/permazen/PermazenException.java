
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

/**
 * Superclass of all unchecked exceptions thrown by the {@link Permazen} API.
 */
@SuppressWarnings("serial")
public class PermazenException extends RuntimeException {

    public PermazenException() {
    }

    public PermazenException(String message) {
        super(message);
    }

    public PermazenException(Throwable cause) {
        super(cause);
    }

    public PermazenException(String message, Throwable cause) {
        super(message, cause);
    }
}

