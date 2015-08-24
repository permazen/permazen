
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.jsimpledb.util.ByteWriter;

/**
 * The {@link FieldType} for {@link ReferenceField}s. Instances support object type restriction.
 *
 * <p>
 * Null values are supported by this class.
 * </p>
 */
public class ReferenceFieldType extends NullSafeType<ObjId> {

    private final TreeSet<Integer> objectTypes;

    /**
     * Constructor.
     *
     * <p>
     * No restrictions will be placed on encoded references.
     * </p>
     */
    public ReferenceFieldType() {
        this(null);
    }

    /**
     * Constructor.
     *
     * @param objectTypes allowed object type storage IDs, or null for no restriction
     */
    public ReferenceFieldType(Set<Integer> objectTypes) {
        super(FieldType.REFERENCE_TYPE_NAME, FieldTypeRegistry.OBJ_ID);
        this.objectTypes = objectTypes != null ? new TreeSet<>(objectTypes) : null;
    }

    /**
     * Get the object types this field type is allowed to reference, if so restricted.
     *
     * @return storage IDs of allowed object types, or null if there is no restriction
     */
    public SortedSet<Integer> getObjectTypes() {
        return objectTypes != null ? Collections.unmodifiableSortedSet(this.objectTypes) : null;
    }

// FieldType

    @Override
    public void write(ByteWriter writer, ObjId id) {
        super.write(writer, this.checkAllowed(id));
    }

    @Override
    public ObjId validate(Object obj) {
        return this.checkAllowed(super.validate(obj));
    }

    /**
     * Verify the reference value is permitted by this instance.
     *
     * @param id reference value
     * @return validated reference
     * @throws InvalidReferenceException if {@code id} has a storage ID that is not allowed by this instance
     */
    protected ObjId checkAllowed(ObjId id) {
        if (this.objectTypes != null && id != null && !this.objectTypes.contains(id.getStorageId()))
            throw new InvalidReferenceException(id, this.objectTypes);
        return id;
    }

// Object

    @Override
    public String toString() {
        String desc = super.toString();
        if (this.objectTypes != null)
            desc += " to " + this.objectTypes;
        return desc;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ReferenceFieldType that = (ReferenceFieldType)obj;
        return this.objectTypes != null ? this.objectTypes.equals(that.objectTypes) : that.objectTypes == null;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.objectTypes != null ? this.objectTypes.hashCode() : 0);
    }
}

