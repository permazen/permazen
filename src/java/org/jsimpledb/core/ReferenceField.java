
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Set;
import java.util.SortedSet;

/**
 * A field that references another {@link Database} object.
 *
 * <p>
 * Null values sort last.
 * </p>
 *
 * <p>
 * Reference fields are always indexed.
 * </p>
 */
public class ReferenceField extends SimpleField<ObjId> {

    final DeleteAction onDelete;
    final boolean cascadeDelete;

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param version schema version
     * @param onDelete deletion behavior
     * @param cascadeDelete whether to cascade deletes
     * @param objectTypes allowed object type storage IDs, or null for no restriction
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is invalid
     */
    ReferenceField(String name, int storageId, SchemaVersion version,
      DeleteAction onDelete, boolean cascadeDelete, Set<Integer> objectTypes) {
        super(name, storageId, version, new ReferenceFieldType(objectTypes), true);
        if (onDelete == null)
            throw new IllegalArgumentException("null onDelete");
        this.onDelete = onDelete;
        this.cascadeDelete = cascadeDelete;
    }

// Public methods

    /**
     * Get the desired behavior when an object referred to by this field is deleted.
     *
     * @return desired behavior when a referenced object is deleted
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }

    /**
     * Determine whether the referred-to object should be deleted when an object containing this field is deleted.
     *
     * @return whether deletion should cascade to the referred-to object
     */
    public boolean isCascadeDelete() {
        return this.cascadeDelete;
    }

    /**
     * Get the object types this field is allowed to reference, if so restricted.
     *
     * @return storage IDs of allowed object types, or null if there is no restriction
     */
    public SortedSet<Integer> getObjectTypes() {
        return ((ReferenceFieldType)this.getFieldType()).getObjectTypes();
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseReferenceField(this);
    }

    @Override
    public String toString() {
        return "reference field `" + this.name + "'";
    }

// Non-public methods

    @Override
    ReferenceFieldStorageInfo toStorageInfo() {
        return new ReferenceFieldStorageInfo(this, this.parent != null ? this.parent.storageId : 0);
    }
}

