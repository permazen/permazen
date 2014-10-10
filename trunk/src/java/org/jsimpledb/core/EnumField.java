
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.List;

/**
 * A field that contains one entry in an ordered list of unique {@link String} identifiers.
 *
 * <p>
 * Two instances of this class are considered compatible only when their ordered lists of identifiers are identical.
 * </p>
 */
public class EnumField extends SimpleField<EnumValue> {

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param version schema version
     * @param typeName {@link Enum} class type name
     * @param idents the unique enum identifiers
     * @param indexed whether this field is indexed
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is invalid
     * @throws IllegalArgumentException if any identifier in {@code idents} is null, duplicate, or not a valid Java identifier
     */
    EnumField(String name, int storageId, SchemaVersion version, boolean indexed, String typeName, List<String> idents) {
        super(name, storageId, version, EnumFieldType.create(typeName, idents), indexed);
    }

// Public methods

    /**
     * Get the {@link EnumFieldType} associated with this instance.
     */
    @Override
    public EnumFieldType getFieldType() {
        return (EnumFieldType)super.getFieldType();
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseEnumField(this);
    }

    @Override
    public String toString() {
        return "enum field `" + this.name + "'";
    }

// Non-public methods

    @Override
    EnumFieldStorageInfo toStorageInfo() {
        return new EnumFieldStorageInfo(this, this.parent != null ? this.parent.storageId : 0,
          this.getFieldType().getIdentifiers());
    }
}

