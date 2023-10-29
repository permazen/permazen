
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.encoding.Encoding;
import io.permazen.core.encoding.NullSafeEncoding;
import io.permazen.util.ByteWriter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
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
public class ReferenceEncoding extends NullSafeEncoding<ObjId> {

    private static final long serialVersionUID = -5980288575339951079L;

    private final TreeSet<Integer> objectTypes;

    /**
     * Constructor.
     *
     * <p>
     * No restrictions will be placed on encoded references.
     */
    public ReferenceEncoding() {
        this(null);
    }

    /**
     * Constructor.
     *
     * @param objectTypes allowed object type storage IDs, or null for no restriction
     */
    public ReferenceEncoding(Set<Integer> objectTypes) {
        super(null, Encodings.OBJ_ID);
        this.objectTypes = objectTypes != null ? new TreeSet<>(objectTypes) : null;
    }

    /**
     * Get the object types this encoding is allowed to reference, if so restricted.
     *
     * @return storage IDs of allowed object types, or null if there is no restriction
     */
    public SortedSet<Integer> getObjectTypes() {
        return objectTypes != null ? Collections.unmodifiableSortedSet(this.objectTypes) : null;
    }

// Encoding

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

    @Override
    public ReferenceEncoding genericizeForIndex() {
        return this.objectTypes != null ? new ReferenceEncoding() : this;
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
        final ReferenceEncoding that = (ReferenceEncoding)obj;
        return Objects.equals(this.objectTypes, that.objectTypes);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.objectTypes);
    }
}
