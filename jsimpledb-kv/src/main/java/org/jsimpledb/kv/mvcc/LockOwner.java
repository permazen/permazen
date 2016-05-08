
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mvcc;

import java.util.HashSet;

/**
 * Represents the owner of a {@link Lock} managed by a {@link LockManager}.
 *
 * <p>
 * Each instance of this class represents a separate lock owner.
 * </p>
 *
 * @see LockManager
 */
public final class LockOwner {

    final HashSet<Lock> locks = new HashSet<>();

    /**
     * Constructor.
     */
    public LockOwner() {
    }
}

