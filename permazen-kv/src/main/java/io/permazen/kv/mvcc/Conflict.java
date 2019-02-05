
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

/**
 * Represents an MVCC conflict.
 *
 * <p>
 * A MVCC conflict occurs when a key (or key range) that was read in one transaction was also somehow modified in another,
 * simultaneous transaction.
 *
 * <p>
 * Instances are immutable.
 *
 * @see Reads#getAllConflicts Reads.getAllConflicts()
 */
public abstract class Conflict {

    Conflict() {
    }
}
