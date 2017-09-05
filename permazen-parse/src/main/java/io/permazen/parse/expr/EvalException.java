
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

/**
 * Thrown when an error occurs evaluating an expression.
 */
@SuppressWarnings("serial")
public class EvalException extends RuntimeException {

    public EvalException() {
    }

    public EvalException(String message) {
        super(message);
    }

    public EvalException(Throwable cause) {
        super(cause);
    }

    public EvalException(String message, Throwable cause) {
        super(message, cause);
    }
}

