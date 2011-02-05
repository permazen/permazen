
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

/**
 * Exception that can be thrown by {@link SelfValidating#checkValid}.
 *
 * <p>
 * Instances will be automatically caught and converted into a constraint violation using the exception's message
 * if any, otherwise the default message.
 * </p>
 *
 * @see SelfValidating
 */
@SuppressWarnings("serial")
public class SelfValidationException extends Exception {

    public SelfValidationException() {
    }

    public SelfValidationException(String message) {
        super(message);
    }
}

