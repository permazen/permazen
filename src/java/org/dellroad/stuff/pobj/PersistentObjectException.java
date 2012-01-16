
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

/**
 * Runtime exception thrown during {@link PersistentObject} operations.
 */
@SuppressWarnings("serial")
public class PersistentObjectException extends RuntimeException {

    public PersistentObjectException() {
    }

    public PersistentObjectException(String message) {
        super(message);
    }

    public PersistentObjectException(String message, Throwable cause) {
        super(message, cause);
    }

    public PersistentObjectException(Throwable cause) {
        super(cause);
    }
}

