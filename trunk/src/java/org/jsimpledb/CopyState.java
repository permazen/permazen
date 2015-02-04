
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.TreeMap;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjIdSet;

/**
 * Internal state used by {@link JTransaction#copyTo}.
 */
class CopyState {

    private final TreeMap<int[], ObjIdSet> traversedMap = new TreeMap<>(Ints.lexicographicalComparator());
    private final ObjIdSet copied;

    CopyState() {
        this(new ObjIdSet());
    }

    CopyState(ObjIdSet copied) {
        this.copied = copied;
    }

    /**
     * Determine if an object has already been copied, and if not mark it so.
     *
     * @param id object ID of object being copied
     * @return true if {@code id} was not previously marked copied, otherwise false
     */
    public boolean markCopied(ObjId id) {
        return this.copied.add(id);
    }

    /**
     * Determine if the specified reference path has already been traversed starting at the given object, and if not mark it so.
     *
     * @param id object ID of object being copied
     * @param fields reference path storage IDs
     * @return true if {@code fields} was not previously marked as traversed from {@code id}, otherwise false
     */
    public boolean markTraversed(ObjId id, int[] fields) {

        // We also mark all prefixes of the given reference path
        boolean marked = false;
        for (int limit = fields.length; limit > 0; limit--) {
            final boolean fullPath = limit == fields.length;
            final int[] prefix = fullPath ? fields : Arrays.copyOfRange(fields, 0, limit);
            ObjIdSet idSet = this.traversedMap.get(prefix);
            if (idSet == null) {
                idSet = new ObjIdSet();
                this.traversedMap.put(prefix, idSet);
            }
            final boolean prefixMarked = idSet.add(id);
            if (fullPath)
                marked = prefixMarked;
        }
        return marked;
    }
}

