
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

/**
 * Wraps checked exceptions so they can be thrown across API methods that don't declare them.
 */
@SuppressWarnings("serial")
public class CheckedExceptionWrapper extends RuntimeException {

    private final Exception exception;

    /**
     * Constructor.
     *
     * @throws IllegalArgumentException if {@code exception} is {@code null}
     */
    public CheckedExceptionWrapper(Exception exception) {
        if (exception == null)
            throw new IllegalArgumentException("null exception");
        this.exception = exception;
    }

    /**
     * Get the wrapped exception.
     */
    public Exception getException() {
        return this.exception;
    }

    /**
     * Throw the wrapped exception.
     */
    public void throwException() throws Exception {
        throw this.exception;
    }
}

