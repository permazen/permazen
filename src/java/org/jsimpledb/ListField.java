
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.List;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * List field.
 *
 * @param <E> Java type for the list elements
 */
public class ListField<E> extends CollectionField<List<E>, E> {

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
    ListField(String name, int storageId, SchemaVersion version, SimpleField<E> elementField) {
        super(name, storageId, version, new TypeToken<List<E>>() { }
          .where(new TypeParameter<E>() { }, elementField.typeToken.wrap()), elementField);
    }

    @Override
    public List<E> getValue(Transaction tx, ObjId id) {
        return new JSList<E>(tx, this, id);
    }

    @Override
    ListFieldStorageInfo toStorageInfo() {
        return new ListFieldStorageInfo(this);
    }

    @Override
    public String toString() {
        return "list field `" + this.name + "' of " + this.elementField.fieldType;
    }

// Subclass Methods

    @Override
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer) {
        writer.write(value);
        id.writeTo(writer);
        writer.write(reader);
    }
}

