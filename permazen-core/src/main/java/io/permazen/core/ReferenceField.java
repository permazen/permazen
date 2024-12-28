
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.core.util.ObjIdMap;
import io.permazen.schema.ReferenceSchemaField;
import io.permazen.util.ByteData;

import java.util.Collections;
import java.util.Set;

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

    ReferenceField(ObjType objType, ReferenceSchemaField field, Set<ObjType> objTypes) {
        super(objType, field, new ReferenceEncoding(objType.getSchema(), objTypes), true);
        this.inverseDelete = field.getInverseDelete();
        this.forwardDelete = field.isForwardDelete();
        this.allowDeleted = field.isAllowDeleted();
        assert this.inverseDelete != null;
    }

// Public methods

    @Override
    public ReferenceEncoding getEncoding() {
        return (ReferenceEncoding)super.getEncoding();
    }

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
     * Determine whether this field accepts references to deleted objects in normal (non-detached) transactions.
     *
     * @return whether deleted objects are allowed in normal transactions
     */
    public boolean isAllowDeleted() {
        return this.allowDeleted;
    }

    /**
     * Get the object types this field is allowed to reference, if so restricted.
     *
     * @return names of allowed object types, or null if there is no restriction
     */
    public Set<String> getObjectTypes() {
        final Set<String> objectTypeNames = ((ReferenceEncoding)this.encoding).getObjectTypeNames();
        return objectTypeNames != null ? Collections.unmodifiableSet(objectTypeNames) : null;
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseReferenceField(this);
    }

    @Override
    public String toString() {
        return "reference field \"" + this.getFullName() + "\"";
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
        Preconditions.checkArgument(dstId != null, String.format("can't copy %s because %s is remapped to null", srcId, srcId));
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
        final ByteData value = srcTx.kvt.get(this.buildKey(id));
        if (value == null)
            return;
        dstTx.checkDeletedAssignment(id, this, this.encoding.read(value.newReader()));
    }
}
