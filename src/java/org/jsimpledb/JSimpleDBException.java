
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

/**
 * Superclass of all unchecked exceptions thrown by the {@link JSimpleDB} API.
 */
@SuppressWarnings("serial")
public class JSimpleDBException extends RuntimeException {

    public JSimpleDBException() {
    }

    public JSimpleDBException(String message) {
        super(message);
    }

    public JSimpleDBException(Throwable cause) {
        super(cause);
    }

    public JSimpleDBException(String message, Throwable cause) {
        super(message, cause);
    }
}

