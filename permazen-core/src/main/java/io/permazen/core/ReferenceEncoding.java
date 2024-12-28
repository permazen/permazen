
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.encoding.NullSafeEncoding;
import io.permazen.util.ByteData;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The {@link Encoding} for {@link ReferenceField}s. Instances support object type restriction.
 *
 * <p>
 * Binary encoding uses the value from {@link ObjId#getBytes}, or {@code 0xff} to represent null.
 *
 * <p>
 * Null values are supported by this class.
 */
@SuppressWarnings("serial")
public class ReferenceEncoding extends NullSafeEncoding<ObjId> {

    private static final long serialVersionUID = -5980288575339951079L;

    private final Schema schema;
    private final Set<ObjType> objectTypes;
    private final TreeSet<String> objectTypeNames;
    private final HashSet<Integer> objectTypeStorageIds;

    /**
     * Constructor.
     *
     * <p>
     * No restrictions will be placed on encoded references.
     */
    public ReferenceEncoding() {
        super(null, Encodings.OBJ_ID);
        this.schema = null;
        this.objectTypes = null;
        this.objectTypeNames = null;
        this.objectTypeStorageIds = null;
    }

// Public Methods

    /**
     * Constructor.
     *
     * @param schema associated schema
     * @param objectTypes allowed object type storage IDs, or null for no restriction
     * @throws IllegalArgumentException if {@code schema} is null
     */
    public ReferenceEncoding(Schema schema, Set<ObjType> objectTypes) {
        super(null, Encodings.OBJ_ID);
        Preconditions.checkArgument(schema != null, "null schema");
        this.schema = schema;
        this.objectTypes = objectTypes;
        if (this.objectTypes != null) {
            this.objectTypeNames = new TreeSet<>();
            this.objectTypeStorageIds = new HashSet<>(this.objectTypes.size());
            for (ObjType objType : this.objectTypes) {
                this.objectTypeNames.add(objType.getName());
                this.objectTypeStorageIds.add(objType.getStorageId());
            }
        } else {
            this.objectTypeNames = null;
            this.objectTypeStorageIds = null;
        }
    }

    /**
     * Get the object types this encoding is allowed to reference, if so restricted.
     *
     * @return allowed object types, or null if there is no restriction
     */
    public Set<ObjType> getObjectTypes() {
        return this.objectTypes;
    }

// Encoding

    @Override
    public void write(ByteData.Writer writer, ObjId id) {
        super.write(writer, this.checkAllowed(id));
    }

    @Override
    public ObjId validate(Object obj) {
        return this.checkAllowed(super.validate(obj));
    }

    /**
     * Verify the reference target is permitted by this instance.
     *
     * @param id reference value
     * @return {@code id} after successful validation
     * @throws InvalidReferenceException if {@code id} has a storage ID that is not allowed by this instance
     */
    public ObjId checkAllowed(ObjId id) {

        // Are there any restrictions?
        if (id == null || this.objectTypes == null)
            return id;

        // Check object type vs. restrictions
        final int storageId = id.getStorageId();
        if (this.objectTypeStorageIds.contains(storageId))
            return id;

        // Build and throw exception
        String typeName;
        try {
            typeName = this.schema.getObjType(storageId).getName();
        } catch (UnknownTypeException e) {
            typeName = "(Unknown)";
        }
        throw new InvalidReferenceException(id, typeName, this.objectTypeNames);
    }

    /**
     * Attempt to convert a value from the given {@link Encoding} into a value of this {@link Encoding}.
     *
     * <p>
     * The only conversion supported by {@link ReferenceEncoding} is to/from {@link ObjId}.
     */
    @Override
    public <S> ObjId convert(Encoding<S> type, S value) {
        return this.validate(value);
    }

// Package Methods

    Set<String> getObjectTypeNames() {
        return this.objectTypeNames;
    }

    Set<Integer> getObjectTypeStorageIds() {
        return this.objectTypeStorageIds;
    }

    ReferenceEncoding genericizeForIndex() {
        return this.objectTypes == null ? this : new ReferenceEncoding();
    }

// Object

    @Override
    public String toString() {
        String desc = super.toString();
        if (this.objectTypeNames != null)
            desc += " to " + this.objectTypeNames;
        return desc;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ReferenceEncoding that = (ReferenceEncoding)obj;
        return Objects.equals(this.objectTypeStorageIds, that.objectTypeStorageIds);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.objectTypeStorageIds);
    }
}
