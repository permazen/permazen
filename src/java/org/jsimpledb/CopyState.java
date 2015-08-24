
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ObjIdSet;

/**
 * Keeps tracks of which objects have already been copied when copying objects between transactions.
 *
 * @see JObject#copyTo JObject.copyTo()
 * @see JTransaction#copyTo(JTransaction, JObject, ObjId, CopyState, String...) JTransaction.copyTo()
 */
public class CopyState implements Cloneable {

    private final TreeMap<int[], ObjIdSet> traversedMap = new TreeMap<>(Ints.lexicographicalComparator());
    private /*final*/ ObjIdSet copied;

    /**
     * Default constructor.
     */
    public CopyState() {
        this(new ObjIdSet());
    }

    /**
     * Constructor to use when a set of known objects has already been copied.
     *
     * @param copied the ID's of objects that have already been copied
     * @throws IllegalArgumentException if {@code copied} is null
     */
    public CopyState(ObjIdSet copied) {
        Preconditions.checkArgument(copied != null, "null copied");
        this.copied = copied.clone();
        synchronized (this) { }
    }

    /**
     * Determine if an object has already been copied, and if not mark it so.
     *
     * @param id object ID of object being copied
     * @return true if {@code id} was not previously marked copied, otherwise false
     * @throws IllegalArgumentException if {@code id} is null
     */
    public boolean markCopied(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return this.copied.add(id);
    }

    /**
     * Determine if the specified reference path has already been traversed starting at the given object, and if not mark it so.
     *
     * @param id object ID of object being copied
     * @param fields reference path storage IDs
     * @return true if {@code fields} was not previously marked as traversed from {@code id}, otherwise false
     * @throws IllegalArgumentException if either parameter is null
     * @throws IllegalArgumentException if {@code fields} has length zero
     */
    public boolean markTraversed(ObjId id, int[] fields) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(fields != null, "null fields");
        Preconditions.checkArgument(fields.length > 0, "empty fields");

        // Mark the path, and all prefixes of the path, as having been traversed
        boolean fullPath = true;
        for (int limit = fields.length; limit > 0; limit--) {
            final int[] prefix = fullPath ? fields : Arrays.copyOfRange(fields, 0, limit);
            ObjIdSet idSet = this.traversedMap.get(prefix);
            if (idSet == null) {
                idSet = new ObjIdSet();
                this.traversedMap.put(prefix, idSet);
            }
            if (!idSet.add(id))                         // we have already marked this prefix (and every shorter prefix)
                return !fullPath;
            fullPath = false;
        }

        // Done
        return true;
    }

// Cloneable

    @Override
    public CopyState clone() {
        final CopyState clone;
        try {
            clone = (CopyState)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        for (Map.Entry<int[], ObjIdSet> entry : this.traversedMap.entrySet())
            clone.traversedMap.put(entry.getKey(), entry.getValue().clone());
        clone.copied = this.copied.clone();
        return clone;
    }
}

