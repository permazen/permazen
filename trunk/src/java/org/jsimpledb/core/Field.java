
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Represents a field in an {@link ObjType} or a ({@linkplain SimpleField simple}) sub-field of a {@link ComplexField}.
 *
 * @param <T> Java type for the field's values
 */
public abstract class Field<T> extends SchemaItem {

    final TypeToken<T> typeToken;

    /**
     * Constructor for normal fields of an {@link ObjType}.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param version schema version
     * @param typeToken Java type for the field's values
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    Field(String name, int storageId, SchemaVersion version, TypeToken<T> typeToken) {
        super(name, storageId, version);
        if (typeToken == null)
            throw new IllegalArgumentException("null typeToken");
        this.typeToken = typeToken;
    }

// Public methods

    /**
     * Get the Java type corresponding to this field.
     */
    public TypeToken<T> getTypeToken() {
        return this.typeToken;
    }

    /**
     * Get the Java value of this field in the given object.
     * Does not alter the schema version of the object.
     *
     * @param tx transaction
     * @param id object id
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if either parameter is null
     */
    public abstract T getValue(Transaction tx, ObjId id);

    /**
     * Determine if this field in the specified object has its default value in the specified {@link Transaction}.
     *
     * @param tx {@link Transaction} containing field state
     * @param id object ID
     * @return true if this field is set to its initial default value in object {@code id}, otherwise false
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if either parameter is null
     */
    public abstract boolean hasDefaultValue(Transaction tx, ObjId id);

    /**
     * Apply visitor pattern.
     *
     * @param target target to invoke
     * @return value from the method of {@code target} corresponding to this instance's type
     * @throws NullPointerException if {@code target} is null
     */
    public abstract <R> R visit(FieldSwitch<R> target);

// Non-public methods

    /**
     * Create corresponding {@link StorageInfo} object.
     */
    abstract FieldStorageInfo toStorageInfo();

    /**
     * Build the key (or key prefix) for this field in the given object.
     */
    byte[] buildKey(ObjId id) {
        return Field.buildKey(id, this.storageId);
    }

    /**
     * Build the key (or key prefix) for this field in the given object.
     */
    static byte[] buildKey(ObjId id, int storageId) {
        final ByteWriter writer = new ByteWriter();
        id.writeTo(writer);
        UnsignedIntEncoder.write(writer, storageId);
        return writer.getBytes();
    }

    /**
     * Determine if this field and the given field are exactly equivalent.
     */
    abstract boolean isEquivalent(Field<?> field);
}

