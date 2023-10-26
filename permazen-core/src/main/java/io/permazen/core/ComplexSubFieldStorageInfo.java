
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.type.ReferenceFieldType;
import io.permazen.util.ByteWriter;
import io.permazen.util.UnsignedIntEncoder;

import java.util.NavigableSet;

abstract class ComplexSubFieldStorageInfo<T, P extends ComplexField<?>> extends SimpleFieldStorageInfo<T> {

    final P parentRepresentative;

    private final int storageIdEncodedLength;

    ComplexSubFieldStorageInfo(SimpleField<T> field, P parent) {
        super(field);
        assert parent != null;
        assert parent == field.parent;
        this.parentRepresentative = parent;
        this.storageIdEncodedLength = UnsignedIntEncoder.encodeLength(this.storageId);
    }

    @Override
    void unreferenceAll(Transaction tx, ObjId target, NavigableSet<ObjId> referrers) {

        // Sanity check
        assert this.fieldType instanceof ReferenceFieldType;

        // Build the index entry prefix common to all referrers' index entries
        final ByteWriter writer = new ByteWriter(this.storageIdEncodedLength + ObjId.NUM_BYTES * 2);
        UnsignedIntEncoder.write(writer, this.storageId);
        target.writeTo(writer);
        final int mark = writer.mark();

        // Iterate over referrers, extend index entry, and let sub-class do the rest
        for (ObjId referrer : referrers) {
            writer.reset(mark);
            referrer.writeTo(writer);
            this.unreference(tx, target, referrer, writer.getBytes());
        }
    }

    /**
     * Remove references in this sub-field in the specified referrer object to the specified target.
     * Used to implement {@link DeleteAction#UNREFERENCE}.
     *
     * @param tx transaction
     * @param target referenced object being deleted
     * @param referrer object containing this field which refers to {@code target}
     * @param prefix (possibly partial) index entry containing {@code target} and {@code referrer}
     */
    abstract void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix);

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ComplexSubFieldStorageInfo<?, ?> that = (ComplexSubFieldStorageInfo<?, ?>)obj;
        return this.parentRepresentative.storageId == that.parentRepresentative.storageId;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.parentRepresentative.storageId;
    }
}

