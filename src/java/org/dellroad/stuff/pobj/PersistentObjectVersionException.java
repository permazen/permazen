
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

/**
 * Optimistic locking exception thrown by {@link PersistentObject#setRoot PersistentObject.setRoot()}
 * when the expected version number does not agree.
 */
@SuppressWarnings("serial")
public class PersistentObjectVersionException extends PersistentObjectException {

    private final long actualVersion;
    private final long expectedVersion;

    public PersistentObjectVersionException(long actualVersion, long expectedVersion) {
        super("expected version " + expectedVersion + " but actual version was " + actualVersion);
        this.actualVersion = actualVersion;
        this.expectedVersion = expectedVersion;
    }

    /**
     * Get the actual, unexpected version number.
     */
    public long getActualVersion() {
        return this.actualVersion;
    }

    /**
     * Get the version number that was expected.
     */
    public long getExpectedVersion() {
        return this.expectedVersion;
    }
}

