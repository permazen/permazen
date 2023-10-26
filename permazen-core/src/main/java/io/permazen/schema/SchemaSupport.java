
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.core.Database;
import io.permazen.util.AbstractXMLStreaming;

/**
 * Models one Permazen {@link Database} schema version.
 */
class SchemaSupport extends AbstractXMLStreaming implements Cloneable {

    /*final*/ boolean lockedDown;

// Lockdown

    /**
     * Lock down this instance.
     *
     * <p>
     * Once locked down, any attempts to modify this instance (and all associated objects) will result
     * in a {@link IllegalStateException}.
     */
    public void lockDown() {
        if (!this.lockedDown) {
            this.lockDownRecurse();
            this.lockedDown = true;
        }
    }

    /**
     * Determine whether this instance is locked down.
     *
     * @return true if instance is locked down, otherwise false
     */
    public boolean isLockedDown() {
        return this.lockedDown;
    }

    void lockDownRecurse() {
    }

    void verifyNotLockedDown() {
        if (this.lockedDown)
            throw new UnsupportedOperationException("instance is locked down");
    }

// Cloneable

    /**
     * Deep-clone this instance.
     *
     * <p>
     * The returned instance will not be {@linkplain #lockDown locked down}.
     */
    @Override
    protected SchemaSupport clone() {
        final SchemaSupport clone;
        try {
            clone = (SchemaSupport)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.lockedDown = false;
        return clone;
    }
}
