
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.KVStore;

/**
 * Represents an MVCC conflict in which a key that was read in one transaction was adjusted via
 * {@link KVStore#adjustCounter KVStore.adjustCounter()} in another, simultaneous transaction.
 *
 * <p>
 * Instances are immutable.
 *
 * @see Reads#getAllConflicts Reads.getAllConflicts()
 */
public class ReadAdjustConflict extends SingleKeyConflict {

    /**
     * Constructor.
     *
     * <p>
     * Note: the {@code key} is not copied, so the caller should not modify the data therein.
     *
     * @param key the conflicting key
     * @throws IllegalArgumentException if {@code key} is null
     */
    public ReadAdjustConflict(byte[] key) {
        super("read/adjust", key);
    }
}
