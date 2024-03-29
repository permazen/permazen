
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;

import io.permazen.core.ObjId;
import io.permazen.core.Transaction;

import java.util.HashMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Cache for randomly generated object IDs based on unique strings.
 *
 * <p>
 * Instances are thread safe.
 *
 * @see XMLObjectSerializer
 */
@ThreadSafe
public class GeneratedIdCache {

    @GuardedBy("this")
    private final HashMap<String, ObjId> map = new HashMap<>();

    /**
     * Generate an object ID for the given object type name and string.
     *
     * <p>
     * If an {@link ObjId} has already been created for the specified {@code typeName} and {@code string} by this instance,
     * it will be returned.
     * Otherwise, a random {@link ObjId} for which no object exists in the specified {@link Transaction} is generated
     * and returned.
     *
     * @param tx transaction from which to allocate new object IDs
     * @param typeName object type name
     * @param string unique string
     * @return unique, unallocated object ID corresponding to {@code typeName} and {@code string}
     * @throws IllegalArgumentException if any parameter is null
     */
    public synchronized ObjId getGeneratedId(Transaction tx, String typeName, String string) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(typeName != null, "null typeName");
        Preconditions.checkArgument(string != null, "null string");
        final String key = typeName + ":" + string;
        return this.map.computeIfAbsent(key, k -> tx.generateId(typeName));
    }

    /**
     * Clear this instance.
     */
    public synchronized void clear() {
        this.map.clear();
    }
}
