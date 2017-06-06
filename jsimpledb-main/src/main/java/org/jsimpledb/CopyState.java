
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import net.jcip.annotations.NotThreadSafe;

import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ReferenceField;
import org.jsimpledb.core.util.ObjIdMap;
import org.jsimpledb.core.util.ObjIdSet;

/**
 * Keeps tracks of which objects have already been copied when copying objects between transactions.
 *
 * <p>
 * Instances are not thread safe.
 *
 * @see JObject#copyTo JObject.copyTo()
 * @see JTransaction#copyTo(JTransaction, JObject, ObjId, CopyState, String...) JTransaction.copyTo()
 */
@NotThreadSafe
public class CopyState implements Cloneable {

    // Hitch-hiker used by JTransaction.copyTo(). Maps referred-to-but-deleted object -> referring object & field
    /*final*/ ObjIdMap<DeletedAssignment> deletedAssignments = new ObjIdMap<>();

    private final TreeMap<int[], ObjIdSet> traversedMap = new TreeMap<>(Ints.lexicographicalComparator());
    private /*final*/ ObjIdSet copied;
    private boolean suppressNotifications;

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
     * Determine if an object has been marked as copied.
     *
     * @param id object id
     * @return true if {@code id} has been marked copied, otherwise false
     * @throws IllegalArgumentException if {@code id} is null
     */
    public boolean isCopied(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return this.copied.contains(id);
    }

    /**
     * Determine whether to suppress {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnChange} notifications in the destination transaction.
     *
     * <p>
     * Note that for notifications to be delivered in a {@link SnapshotJTransaction}, these annotations must
     * also have {@code snapshotTransactions = true}, even if this property is set to false.
     *
     * <p>
     * Default is false.
     *
     * @return true if {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and
     *  {@link org.jsimpledb.annotation.OnCreate &#64;OnChange} notifications should be suppressed, otherwise false
     */
    public boolean isSuppressNotifications() {
        return this.suppressNotifications;
    }

    /**
     * Configure whether to suppress {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnChange} notifications in the destination transaction.
     *
     * <p>
     * Note that for notifications to be delivered in a {@link SnapshotJTransaction}, these annotations must
     * also have {@code snapshotTransactions = true}, even if this property is set to false.
     *
     * @param suppressNotifications true if {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and
     *  {@link org.jsimpledb.annotation.OnCreate &#64;OnChange} notifications should be suppressed, otherwise false
     */
    public void setSuppressNotifications(boolean suppressNotifications) {
        this.suppressNotifications = suppressNotifications;
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

    // Check for any remaining deleted assignments, and remove the deletedAssignments field
    void checkDeletedAssignments(JTransaction jtx) {

        // Get an arbitrary remaining deleted assignment (if any)
        if (this.deletedAssignments.isEmpty())
            return;
        final Map.Entry<ObjId, DeletedAssignment> entry = this.deletedAssignments.entrySet().iterator().next();

        // Throw exception
        final DeletedAssignment deletedAssignment = entry.getValue();
        final ObjId id = deletedAssignment.getId();
        final ReferenceField field = deletedAssignment.getField();
        final ObjId targetId = entry.getKey();
        throw new DeletedObjectException(targetId, "illegal assignment of deleted object " + targetId
          + " (" + jtx.tx.getTypeDescription(targetId) + ") to " + field + " in object " + id
          + " (" + jtx.tx.getTypeDescription(id) + ")");
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
        clone.deletedAssignments = new ObjIdMap<>();                // does not go along
        return clone;
    }
}

