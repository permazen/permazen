
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

/**
 * Exception thrown by {@link QueryList#get} when the list has become invalid.
 *
 * @see AbstractQueryContainer
 * @see QueryList
 */
@SuppressWarnings("serial")
public class InvalidQueryListException extends Exception {

    public InvalidQueryListException() {
    }

    public InvalidQueryListException(String message) {
        super(message);
    }

    public InvalidQueryListException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidQueryListException(Throwable cause) {
        super(cause);
    }
}

