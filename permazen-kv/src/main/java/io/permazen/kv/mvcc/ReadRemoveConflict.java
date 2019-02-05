
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.kv.KeyRange;

/**
 * Represents an MVCC conflict in which a key or range of keys that was read in one transaction was removed
 * in another, simultaneous transaction.
 *
 * <p>
 * Instances are immutable.
 *
 * @see Reads#getAllConflicts Reads.getAllConflicts()
 */
public class ReadRemoveConflict extends Conflict {

    private final KeyRange range;

    /**
     * Constructor.
     *
     * @param range the range of keys that conflict
     * @throws IllegalArgumentException if {@code range} is null
     */
    public ReadRemoveConflict(KeyRange range) {
        Preconditions.checkArgument(range != null, "null range");
        this.range = range;
    }

// Accessors

    /**
     * Get the key range at which the conflict occurred.
     *
     * @return the affected key range
     */
    public KeyRange getKeyRange() {
        return this.range;
    }

// Object

    @Override
    public int hashCode() {
        return this.range.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ReadRemoveConflict that = (ReadRemoveConflict)obj;
        return this.range.equals(that.range);
    }

    @Override
    public String toString() {
        return "read/remove conflict at " + this.range;
    }
}
