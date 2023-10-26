
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.OnCreate;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ObjId;
import io.permazen.core.ReferenceField;
import io.permazen.core.util.ObjIdMap;
import io.permazen.core.util.ObjIdSet;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Maintains state about a multi-object copy operation.
 *
 * <p>
 * This class keeps tracks of which objects have already been copied when copying objects between transactions.
 *
 * <p>
 * In addition, it supports assigning different object ID's to objects as they are copied into the destination
 * transaction.
 *
 * <p>
 * Instances are not thread safe.
 *
 * @see JObject#copyTo JObject.copyTo()
 * @see JObject#cascadeCopyTo JObject.cascadeCopyTo()
 */
@NotThreadSafe
public class CopyState implements Cloneable {

    // Hitch-hiker used by JTransaction.copyTo(). Maps referred-to-but-deleted object -> referring object & field
    /*final*/ ObjIdMap<DeletedAssignment> deletedAssignments = new ObjIdMap<>();

    private final TreeMap<int[], ObjIdSet> traversedMap = new TreeMap<>(Ints.lexicographicalComparator());
    private /*final*/ ObjIdSet copied;
    private /*final*/ ObjIdMap<ObjId> objectIdMap;
    private boolean suppressNotifications;

    /**
     * Default constructor.
     *
     * <p>
     * Object ID's will not be remapped.
     *
     * <p>
     * Equivalent to: {@link #CopyState(ObjIdSet, ObjIdMap) CopyState(new ObjIdSet(), null)}.
     */
    public CopyState() {
        this(new ObjIdSet(), null);
    }

    /**
     * Default remapping constructor.
     *
     * <p>
     * Object ID's will be remapped based on {@code objectIdMap}.
     *
     * <p>
     * Equivalent to: {@link #CopyState(ObjIdSet, ObjIdMap) CopyState(new ObjIdSet(), objectIdMap)}.
     *
     * @param objectIdMap mapping from source object ID to destination object ID,
     *  or null to disable object ID remapping (i.e., use the same ID)
     */
    public CopyState(ObjIdMap<ObjId> objectIdMap) {
        this(new ObjIdSet(), objectIdMap);
    }

    /**
     * Primary constructor.
     *
     * <p>
     * This constructor allows an object ID map to be provided via {@code objectIdMap}, which specifies the
     * destination transaction object ID to use when copying the corresponding source transaction object.
     *
     * @param copied the ID's of objects that have already been copied
     * @param objectIdMap mapping from source object ID to destination object ID,
     *  or null to disable object ID remapping (i.e., use the same ID)
     * @throws IllegalArgumentException if {@code copied} is null
     */
    public CopyState(ObjIdSet copied, ObjIdMap<ObjId> objectIdMap) {
        Preconditions.checkArgument(copied != null, "null copied");
        this.copied = copied;
        this.objectIdMap = objectIdMap;
    }

    /**
     * Determine if an object has already been copied, and if not mark it so.
     *
     * @param id object ID (in the source transaction) of the object being copied
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
     * @param id object ID (in the source transaction) of the object being copied
     * @return true if {@code id} has been marked copied, otherwise false
     * @throws IllegalArgumentException if {@code id} is null
     */
    public boolean isCopied(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return this.copied.contains(id);
    }

    /**
     * Determine whether to suppress {@link OnCreate &#64;OnCreate} and
     * {@link OnChange &#64;OnChange} notifications in the destination transaction.
     *
     * <p>
     * Note that for notifications to be delivered in a {@link SnapshotJTransaction}, these annotations must
     * also have {@code snapshotTransactions = true}, even if this property is set to false.
     *
     * <p>
     * Default is false.
     *
     * @return true if {@link OnCreate &#64;OnCreate} and
     *  {@link OnChange &#64;OnChange} notifications should be suppressed, otherwise false
     */
    public boolean isSuppressNotifications() {
        return this.suppressNotifications;
    }

    /**
     * Configure whether to suppress {@link OnCreate &#64;OnCreate} and
     * {@link OnChange &#64;OnChange} notifications in the destination transaction.
     *
     * <p>
     * Note that for notifications to be delivered in a {@link SnapshotJTransaction}, these annotations must
     * also have {@code snapshotTransactions = true}, even if this property is set to false.
     *
     * @param suppressNotifications true if {@link OnCreate &#64;OnCreate} and
     *  {@link OnChange &#64;OnChange} notifications should be suppressed, otherwise false
     */
    public void setSuppressNotifications(boolean suppressNotifications) {
        this.suppressNotifications = suppressNotifications;
    }

    /**
     * Determine if the specified reference path has already been traversed starting at the given object, and if not mark it so.
     *
     * @param id object ID (in the source transaction) of the object being copied
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
            if (!this.traversedMap.computeIfAbsent(prefix, p -> new ObjIdSet()).add(id))
                return !fullPath;                       // we have already marked this prefix (and every shorter prefix)
            fullPath = false;
        }

        // Done
        return true;
    }

    /**
     * Get the mapping from object ID in the source transaction to object ID to use in the destination transaction.
     *
     * <p>
     * This method returns the {@code objectIdMap} parameter given to the constructor, if any.
     *
     * @return mapping from source transaction object ID to destination transaction object ID, or null if none configured
     */
    public ObjIdMap<ObjId> getObjectIdMap() {
        return this.objectIdMap;
    }

    /**
     * Get the object ID to use in the destination transaction for the copy of the object with the given object ID
     * in the source transaction.
     *
     * <p>
     * The implementation in {@link CopyState} behaves as follows: if a null {@code objectIdMap} parameter was given
     * to the constructor, {@code objectIdMap} does not contain {@code srcId}, or the corresponding value is null,
     * it returns {@code srcId}. Otherwise, the corresponding value from {@code objectIdMap} is returned.
     *
     * @param srcId source transaction object ID
     * @return corresponding destination transaction object ID
     * @throws IllegalArgumentException if {@code srcId} is null
     */
    public ObjId getDestinationId(ObjId srcId) {
        Preconditions.checkArgument(srcId != null);
        if (this.objectIdMap == null)
            return srcId;
        final ObjId dstId = this.objectIdMap.get(srcId);
        return dstId != null ? dstId : srcId;
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
        clone.objectIdMap = this.objectIdMap.clone();
        clone.deletedAssignments = new ObjIdMap<>();                // does not go along
        return clone;
    }
}
