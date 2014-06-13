
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.NavigableSet;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * Set field.
 *
 * @param <E> Java type for the set elements
 */
public class SetField<E> extends CollectionField<NavigableSet<E>, E> {

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field content storage ID
     * @param version schema version
     * @param elementField this field's element sub-field
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    @SuppressWarnings("serial")
    SetField(String name, int storageId, SchemaVersion version, SimpleField<E> elementField) {
        super(name, storageId, version, new TypeToken<NavigableSet<E>>() { }
          .where(new TypeParameter<E>() { }, elementField.typeToken.wrap()), elementField);
    }

// Public methods

    @Override
    @SuppressWarnings("unchecked")
    public NavigableSet<E> getValue(Transaction tx, ObjId id) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        return (NavigableSet<E>)tx.readSetField(id, this.storageId, false);
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseSetField(this);
    }

    @Override
    public String toString() {
        return "set field `" + this.name + "' of " + this.elementField.fieldType;
    }

// Non-public methods

    @Override
    NavigableSet<E> getValueInternal(Transaction tx, ObjId id) {
        return new JSSet<E>(tx, this, id);
    }

    @Override
    SetFieldStorageInfo toStorageInfo() {
        return new SetFieldStorageInfo(this);
    }

    @Override
    boolean hasComplexIndex(SimpleField<?> subField) {
        return false;       // index value = object ID
    }

    @Override
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer) {
        writer.write(reader);
        id.writeTo(writer);
    }
}

