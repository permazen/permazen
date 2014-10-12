
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

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

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param version schema version
     * @param onDelete deletion behavior
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is invalid
     */
    ReferenceField(String name, int storageId, SchemaVersion version, DeleteAction onDelete) {
        super(name, storageId, version, FieldTypeRegistry.REFERENCE, true);
        if (onDelete == null)
            throw new IllegalArgumentException("null onDelete");
        this.onDelete = onDelete;
    }

// Public methods

    /**
     * Get the desired behavior when an object referred to by this field is deleted.
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
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

