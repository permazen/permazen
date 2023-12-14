
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteWriter;
import io.permazen.util.UnsignedIntEncoder;

import java.util.NavigableSet;

abstract class ComplexSubFieldIndex<C, T> extends SimpleIndex<T> {

    final ComplexField<C> parentRepresentative;

// Constructor

    ComplexSubFieldIndex(Schema schema, SimpleSchemaField schemaField,
      ObjType objType, ComplexField<C> parent, SimpleField<T> field) {
        super(schema, schemaField, parent.name + "." + schemaField.getName(), objType, field);
        this.parentRepresentative = parent;
    }

// Package methods

    @Override
    public final CoreIndex<T, ObjId> getIndex(Transaction tx) {
        return new CoreIndex<>(tx.kvt,
          new IndexView<>(UnsignedIntEncoder.encode(this.storageId),
            this.isPrefixModeForIndex(), this.getField().getEncoding(), Encodings.OBJ_ID));
    }

    @Override
    String getFieldDisplayName(SimpleField<?> field) {
        return String.format("%s.%s", field.parent.name, field.name);
    }

    // Does this simple index require prefix mode?
    //  - YES for list element and map value
    //  - NO for set element and map key
    abstract boolean isPrefixModeForIndex();

    @Override
    void unreferenceAll(Transaction tx, ObjId target, NavigableSet<ObjId> referrers) {

        // Sanity check
        assert this.getField() instanceof ReferenceField;

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
     * This is used to implement {@link DeleteAction#UNREFERENCE} in complex fields.
     *
     * @param tx transaction
     * @param target referenced object being deleted
     * @param referrer object containing this field which refers to {@code target}
     * @param prefix (possibly partial) index entry containing {@code target} and {@code referrer}
     */
    abstract void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix);
}
