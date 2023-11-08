
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import io.permazen.core.ObjId;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A queue of {@link ObjId}'s, where {@link ObjId}'s can be added or removed one at a time.
 *
 * <p>
 * Instances allow duplicates.
 *
 * <p>
 * Instances are not thread safe.
 *
 * @see ObjIdQueues
 */
@NotThreadSafe
public interface ObjIdQueue {

    /**
     * Add an {@link ObjId} to this queue.
     *
     * @param id the {@link ObjId} to add
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalStateException if the maximum capacity is exceeded
     */
    void add(ObjId id);

    /**
     * Remove and return the next {@link ObjId} from this queue.
     *
     * @return the next {@link ObjId}, or null if this queue is empty
     */
    ObjId next();

    /**
     * Get the number of {@link ObjId}'s in this queue.
     *
     * @return current size
     */
    int size();

    /**
     * Determine whether this instance is empty.
     *
     * @return true if empty, otherwise false
     */
    boolean isEmpty();
}
