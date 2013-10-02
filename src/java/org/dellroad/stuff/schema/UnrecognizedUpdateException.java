
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

/**
 * Exception thrown by a {@link AbstractSchemaUpdater#initializeAndUpdateDatabase} when it encounters
 * one or more recorded updates that are not recognized.
 */
@SuppressWarnings("serial")
public class UnrecognizedUpdateException extends IllegalStateException {

    public UnrecognizedUpdateException(String message) {
        super(message);
    }

    public UnrecognizedUpdateException(String message, Throwable cause) {
        super(message, cause);
    }
}

