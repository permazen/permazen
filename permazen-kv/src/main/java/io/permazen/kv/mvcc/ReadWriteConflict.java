
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.util.ByteData;

/**
 * Represents an MVCC conflict in which a key that was read in one transaction had its value changed
 * in another, simultaneous transaction.
 *
 * <p>
 * Instances are immutable.
 *
 * @see Reads#getAllConflicts Reads.getAllConflicts()
 */
public class ReadWriteConflict extends SingleKeyConflict {

    /**
     * Constructor.
     *
     * @param key the conflicting key
     * @throws IllegalArgumentException if {@code key} is null
     */
    public ReadWriteConflict(ByteData key) {
        super("read/write", key);
    }
}
