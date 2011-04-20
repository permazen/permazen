
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.servlet;

/**
 * Thrown by {@link BigFatLockHeldAdvice#lockNotHeld} when the lock is not held.
 */
@SuppressWarnings("serial")
public class BigFatLockNotHeldException extends RuntimeException {

    public BigFatLockNotHeldException() {
    }

    public BigFatLockNotHeldException(String message) {
        super(message);
    }
}

