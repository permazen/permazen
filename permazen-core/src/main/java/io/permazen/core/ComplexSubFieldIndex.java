
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.util.NavigableSet;

/**
 * A simple index on a {@link ComplexField} sub-field.
 *
 * @param <C> the type of the complex field
 * @param <T> the type of the indexed sub-field
 */
public abstract class ComplexSubFieldIndex<C, T> extends SimpleIndex<T> {

    final ComplexField<C> parentRepresentative;

// Constructor

    ComplexSubFieldIndex(Schema schema, SimpleSchemaField schemaField,
      ObjType objType, ComplexField<C> parent, SimpleField<T> field) {
        super(schema, schemaField, parent.name + "." + schemaField.getName(), objType, field);
        this.parentRepresentative = parent;
    }

// Package methods

    @Override
    public final CoreIndex1<T, ObjId> getIndex(Transaction tx) {
        return new CoreIndex1<>(tx.kvt,
          new Index1View<>(UnsignedIntEncoder.encode(this.storageId),
            this.isPrefixModeForIndex(), this.getField().getEncoding(), Encodings.OBJ_ID));
    }

    // Does this simple index require prefix mode?
    //  - YES for list element and map value
    //  - NO for set element and map key
    abstract boolean isPrefixModeForIndex();

    @Override
    void unreferenceAll(Transaction tx, boolean remove, ObjId target, NavigableSet<ObjId> referrers) {

        // Sanity check
        assert this.getField() instanceof ReferenceField;

        // Build the index entry prefix common to all referrers' index entries
        final ByteData.Writer writer = ByteData.newWriter(this.storageIdEncodedLength + ObjId.NUM_BYTES * 2);
        UnsignedIntEncoder.write(writer, this.storageId);
        target.writeTo(writer);
        final int mark = writer.size();

        // Iterate over referrers, extend index entry, and let sub-class do the rest
        for (ObjId referrer : referrers) {
            writer.truncate(mark);
            referrer.writeTo(writer);
            this.unreference(tx, remove, target, referrer, writer.toByteData());
        }
    }

    /**
     * Nullify or remove references to the specified target in this sub-field in the specified referrer object.
     *
     * <p>
     * Used to implement {@link DeleteAction#NULLIFY} and {@link DeleteAction#REMOVE} in complex fields.
     *
     * @param tx transaction
     * @param remove true to remove entries in complex sub-fields, false to just nullify references
     * @param target referenced object being deleted
     * @param referrer object containing this field which refers to {@code target}
     * @param prefix (possibly partial) index entry containing {@code target} and {@code referrer}
     */
    abstract void unreference(Transaction tx, boolean remove, ObjId target, ObjId referrer, ByteData prefix);
}
