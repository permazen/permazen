
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.core.util.ObjIdMap;
import io.permazen.util.ByteReader;

import java.util.Set;
import java.util.SortedSet;

/**
 * A field that references another {@link Database} object.
 *
 * <p>
 * Null values sort last.
 *
 * <p>
 * Reference fields are always indexed.
 */
public class ReferenceField extends SimpleField<ObjId> {

    final DeleteAction inverseDelete;
    final boolean forwardDelete;
    final boolean allowDeleted;
    final boolean allowDeletedSnapshot;

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param schema schema version
     * @param inverseDelete inverse deletion behavior
     * @param forwardDelete whether to cascade deletes
     * @param allowDeleted whether to allow assignment to deleted obects in normal transactions
     * @param allowDeletedSnapshot whether to allow assignment to deleted obects in snapshot transactions
     * @param objectTypes allowed object type storage IDs, or null for no restriction
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is invalid
     */
    ReferenceField(String name, int storageId, Schema schema, DeleteAction inverseDelete,
      boolean forwardDelete, boolean allowDeleted, boolean allowDeletedSnapshot, Set<Integer> objectTypes) {
        super(name, storageId, schema, new ReferenceEncoding(objectTypes), true);
        Preconditions.checkArgument(inverseDelete != null, "null inverseDelete");
        this.inverseDelete = inverseDelete;
        this.forwardDelete = forwardDelete;
        this.allowDeleted = allowDeleted;
        this.allowDeletedSnapshot = allowDeletedSnapshot;
    }

// Public methods

    /**
     * Get the desired behavior when an object referred to by this field is deleted.
     *
     * @return desired behavior when a referenced object is deleted
     */
    public DeleteAction getInverseDelete() {
        return this.inverseDelete;
    }

    /**
     * Determine whether the referred-to object should be deleted when an object containing this field is deleted.
     *
     * @return whether deletion should cascade to the referred-to object
     */
    public boolean isForwardDelete() {
        return this.forwardDelete;
    }

    /**
     * Determine whether this field accepts references to deleted objects in normal (non-snapshot) transactions.
     *
     * @return whether deleted objects are allowed in normal transactions
     */
    public boolean isAllowDeleted() {
        return this.allowDeleted;
    }

    /**
     * Determine whether this field accepts references to deleted objects in snapshot transactions.
     *
     * @return whether deleted objects are allowed in snapshot transactions
     */
    public boolean isAllowDeletedSnapshot() {
        return this.allowDeletedSnapshot;
    }

    /**
     * Get the object types this field is allowed to reference, if so restricted.
     *
     * @return storage IDs of allowed object types, or null if there is no restriction
     */
    public SortedSet<Integer> getObjectTypes() {
        return ((ReferenceEncoding)this.encoding).getObjectTypes();
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseReferenceField(this);
    }

    @Override
    public String toString() {
        return "reference field \"" + this.name + "\"";
    }

// Non-public methods

    @Override
    protected boolean remapsObjectId() {
        return true;
    }

    @Override
    protected ObjId remapObjectId(ObjIdMap<ObjId> objectIdMap, ObjId srcId) {
        if (srcId == null || objectIdMap == null || !objectIdMap.containsKey(srcId))
            return srcId;
        final ObjId dstId = objectIdMap.get(srcId);
        Preconditions.checkArgument(dstId != null, "can't copy " + srcId + " because " + srcId + " is remapped to null");
        return dstId;
    }

    /**
     * Find any object referenced by this field in the source transaction that don't exist in the destination transaction.
     * This should work for both normal fields and sub-fields of complex fields.
     *
     * @param srcTx source transaction
     * @param dstTx destination transaction
     * @param id object containing the reference field
     * @see Transaction#checkDeletedAssignment
     */
    void findAnyDeletedAssignments(Transaction srcTx, Transaction dstTx, ObjId id) {

        // Handle complex sub-field case
        if (this.parent != null) {
            for (ObjId targetId : this.parent.iterateSubField(srcTx, id, this))
                dstTx.checkDeletedAssignment(id, this, targetId);
            return;
        }

        // Handle simple field case
        final byte[] value = srcTx.kvt.get(this.buildKey(id));
        if (value == null)
            return;
        dstTx.checkDeletedAssignment(id, this, this.encoding.read(new ByteReader(value)));
    }

    @Override
    boolean isUpgradeCompatible(Field<?> field) {
        if (field.getClass() != this.getClass())
            return false;
        return true;                        // we allow object type restrictions to differ
    }
}
