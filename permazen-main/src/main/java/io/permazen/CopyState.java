
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.OnCreate;
import io.permazen.core.ObjId;
import io.permazen.core.util.ObjIdMap;
import io.permazen.core.util.ObjIdSet;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Holds state during a multi-object copy operation.
 *
 * <p>
 * This class keeps tracks of which objects have already been copied when copying objects between transactions.
 *
 * <p>
 * It also supports assigning different object ID's to objects as they are copied into the destination transaction.
 *
 * <p>
 * Instances are not thread safe.
 *
 * @see JObject#copyTo JObject.copyTo()
 * @see JObject#cascade JObject.cascade()
 */
@NotThreadSafe
public class CopyState {

    private final ObjIdSet copied;
    private final ObjIdMap<ObjId> objectIdMap;
    private boolean suppressNotifications;

// Constructors

    /**
     * Default constructor.
     *
     * <p>
     * No object ID's will be remapped.
     *
     * <p>
     * Equivalent to: {@link #CopyState(ObjIdSet, ObjIdMap) CopyState(new ObjIdSet(), null)}.
     */
    public CopyState() {
        this(new ObjIdSet(), new ObjIdMap<>());
    }

    /**
     * Default remapping constructor.
     *
     * <p>
     * Equivalent to: {@link #CopyState(ObjIdSet, ObjIdMap) CopyState(new ObjIdSet(), objectIdMap)}.
     *
     * @param objectIdMap mapping from source object ID to destination object ID
     * @throws IllegalArgumentException if {@code objectIdMap} is null
     */
    public CopyState(ObjIdMap<ObjId> objectIdMap) {
        this(new ObjIdSet(), objectIdMap);
    }

    /**
     * Primary constructor.
     *
     * <p>
     * Objects whose object ID's are in {@code copied} will not be copied.
     *
     * <p>
     * Otherwise, copied objects will have their object ID's remapped based on {@code objectIdMap}:
     * <ul>
     *  <li>If an object's ID does not exist in {@code objectIdMap}, then the same ID is used for the copy
     *  <li>If an object's ID is mapped to a non-null value in {@code objectIdMap}, then that ID is used for the copy
     *  <li>Otherwise, a new, unused ID is allocated for the copy, and {@code objectIdMap} is updated if/when that occurs
     * </ul>
     *
     * @param copied the ID's of objects that have already been copied
     * @param objectIdMap optional mapping from source object ID to remapped destination object ID
     * @throws IllegalArgumentException if either parameter is null
     */
    public CopyState(ObjIdSet copied, ObjIdMap<ObjId> objectIdMap) {
        Preconditions.checkArgument(copied != null, "null copied");
        Preconditions.checkArgument(objectIdMap != null, "null objectIdMap");
        this.copied = copied;
        this.objectIdMap = objectIdMap;
    }

    /**
     * Copy constructor.
     *
     * @param original instance to copy
     */
    public CopyState(CopyState original) {
        this.copied = original.copied.clone();
        this.objectIdMap = original.objectIdMap.clone();
        this.suppressNotifications = original.suppressNotifications;
    }

// Public Methods

    /**
     * Determine if an object has already been copied, and if not mark it so.
     *
     * @param srcId object ID (in the source transaction) of the object being copied
     * @return true if {@code srcId} was not previously marked copied, otherwise false
     * @throws IllegalArgumentException if {@code srcId} is null
     */
    public boolean markCopied(ObjId srcId) {
        Preconditions.checkArgument(srcId != null, "null srcId");
        return this.copied.add(srcId);
    }

    /**
     * Determine if an object has been marked as copied.
     *
     * @param srcId object ID (in the source transaction) of the object being copied
     * @return true if {@code srcId} has been marked copied, otherwise false
     * @throws IllegalArgumentException if {@code srcId} is null
     */
    public boolean isCopied(ObjId srcId) {
        Preconditions.checkArgument(srcId != null, "null srcId");
        return this.copied.contains(srcId);
    }

    /**
     * Determine whether to suppress {@link OnCreate &#64;OnCreate} and
     * {@link OnChange &#64;OnChange} notifications in the destination transaction.
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
     * @param suppressNotifications true if {@link OnCreate &#64;OnCreate} and
     *  {@link OnChange &#64;OnChange} notifications should be suppressed, otherwise false
     */
    public void setSuppressNotifications(boolean suppressNotifications) {
        this.suppressNotifications = suppressNotifications;
    }

    /**
     * Get the mapping from object ID in the source transaction to object ID to use in the destination transaction.
     *
     * <p>
     * This method returns the {@code objectIdMap} parameter given to the constructor.
     *
     * @return mapping from source transaction object ID to destination transaction object ID
     */
    public ObjIdMap<ObjId> getObjectIdMap() {
        return this.objectIdMap;
    }

    /**
     * Get the object ID for the copy of the given source object.
     *
     * <p>
     * This assumes the object has already been copied, so its new, remapped object ID (if any)
     * has already been updated in the object ID map. This can be verified using {@link #isCopied isCopied()}.
     *
     * @param srcId source transaction object ID of an object that has been copied
     * @return destination transaction object ID corresponding to {@code srcId}, never null
     * @throws IllegalArgumentException if {@code srcId} is null
     */
    public ObjId getDestId(ObjId srcId) {
        Preconditions.checkArgument(srcId != null, "null srcId");
        ObjId dstId = this.objectIdMap.get(srcId);
        return dstId != null ? dstId : srcId;
    }
}
