
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
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
    final byte[] encodedStorageId;

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param schema schema version
     * @param typeToken Java type for the field's values
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    Field(String name, int storageId, Schema schema, TypeToken<T> typeToken) {
        super(name, storageId, schema);
        Preconditions.checkArgument(typeToken != null, "null typeToken");
        this.typeToken = typeToken;
        this.encodedStorageId = UnsignedIntEncoder.encode(this.storageId);
    }

// Public methods

    /**
     * Get the Java type corresponding to this field.
     *
     * @return this field's type
     */
    public TypeToken<T> getTypeToken() {
        return this.typeToken;
    }

    /**
     * Get the value of this field in the given object.
     * Does not alter the schema version of the object.
     *
     * @param tx transaction
     * @param id object id
     * @return this field's value in the specified object
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if this field does not exist in the specified object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if either parameter is null
     * @throws IllegalArgumentException if this field is a sub-field of a {@link ComplexField}
     */
    public abstract T getValue(Transaction tx, ObjId id);

    /**
     * Determine if this field in the specified object has its default value in the specified {@link Transaction}.
     *
     * @param tx {@link Transaction} containing field state
     * @param id object ID
     * @return true if this field is set to its initial default value in object {@code id}, otherwise false
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if either parameter is null
     * @throws IllegalArgumentException if this field is a sub-field of a {@link ComplexField}
     */
    public abstract boolean hasDefaultValue(Transaction tx, ObjId id);

    /**
     * Apply visitor pattern.
     *
     * @param target target to invoke
     * @param <R> visitor return type
     * @return value from the method of {@code target} corresponding to this instance's type
     * @throws NullPointerException if {@code target} is null
     */
    public abstract <R> R visit(FieldSwitch<R> target);

// Non-public methods

    /**
     * Copy field value between transactions.
     *
     * <p>
     * This method assumes both objects exist and both transactions are locked.
     */
    abstract void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx);

    /**
     * Build the key (or key prefix) for this field in the given object.
     */
    byte[] buildKey(ObjId id) {
        final ByteWriter writer = new ByteWriter(ObjId.NUM_BYTES + this.encodedStorageId.length);
        id.writeTo(writer);
        writer.write(this.encodedStorageId);
        return writer.getBytes();
    }

    /**
     * Build the key (or key prefix) for a field with the given storage ID in the given object.
     */
    static byte[] buildKey(ObjId id, int storageId) {
        final ByteWriter writer = new ByteWriter(ObjId.NUM_BYTES + UnsignedIntEncoder.encodeLength(storageId));
        id.writeTo(writer);
        UnsignedIntEncoder.write(writer, storageId);
        return writer.getBytes();
    }

    /**
     * Determine if the given {@link Field} is equivalent to this one, or should be reset on an upgrade.
     */
    abstract boolean isUpgradeCompatible(Field<?> field);
}

