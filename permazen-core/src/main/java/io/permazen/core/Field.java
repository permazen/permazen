
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.util.ObjIdMap;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.schema.SchemaField;
import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

/**
 * A field in an {@link ObjType} or a ({@linkplain SimpleField simple}) sub-field of a {@link ComplexField}
 * in an {@link ObjType}.
 *
 * @param <T> field's value type
 */
public abstract class Field<T> extends SchemaItem {

    final ObjType objType;
    final TypeToken<T> typeToken;
    final ByteData encodedStorageId;

    Field(ObjType objType, SchemaField field, TypeToken<T> typeToken) {
        super(objType.getSchema(), field, field.getName());
        Preconditions.checkArgument(typeToken != null, "null typeToken");
        this.objType = objType;
        this.typeToken = typeToken;
        this.encodedStorageId = UnsignedIntEncoder.encode(this.storageId);
    }

// Public methods

    /**
     * Get the full name of this field.
     *
     * <p>
     * If the field is a sub-field of a complex field, the full name is the field's name qualified
     * by the parent field name, e.g., {@code "mymap.key"}. Otherwise, the full is is the same as the name.
     *
     * @return this field's full name
     */
    public String getFullName() {
        return this.name;
    }

    /**
     * Get the {@link ObjType} that contains this field.
     *
     * @return this field's object type
     */
    public ObjType getObjType() {
        return this.objType;
    }

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
     * Does not alter the schema of the object.
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
     * Get the key in the underlying key/value store corresponding to this field in the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>This method does not check whether the object actually exists.</li>
     *  <li>Complex fields utilize multiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param id object ID
     * @return the {@link KVDatabase} key (or key prefix) for the field in the specified object
     * @throws IllegalArgumentException if {@code id} is null or has the wrong object type
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     */
    public ByteData getKey(ObjId id) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(id.getStorageId() == this.objType.getStorageId(), "invalid id");

        // Build key
        final ByteData.Writer writer = ByteData.newWriter();
        id.writeTo(writer);
        UnsignedIntEncoder.write(writer, this.storageId);
        return writer.toByteData();
    }

// FieldSwitch

    /**
     * Apply visitor pattern.
     *
     * @param target target to invoke
     * @param <R> visitor return type
     * @return return value from the method of {@code target} corresponding to this instance's type
     * @throws NullPointerException if {@code target} is null
     */
    public abstract <R> R visit(FieldSwitch<R> target);

// Package Methods

    /**
     * Copy field value between transactions.
     *
     * <p>
     * This method assumes both objects exist, both transactions are locked, and both objects have this same field.
     */
    abstract void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap);

    /**
     * Build the key (or key prefix) for this field in the given object.
     */
    ByteData buildKey(ObjId id) {
        final ByteData.Writer writer = ByteData.newWriter(ObjId.NUM_BYTES + this.encodedStorageId.size());
        id.writeTo(writer);
        writer.write(this.encodedStorageId);
        return writer.toByteData();
    }

    /**
     * Build the key (or key prefix) for a field with the given storage ID in the given object.
     */
    static ByteData buildKey(ObjId id, int storageId) {
        final ByteData.Writer writer = ByteData.newWriter(ObjId.NUM_BYTES + UnsignedIntEncoder.encodeLength(storageId));
        id.writeTo(writer);
        UnsignedIntEncoder.write(writer, storageId);
        return writer.toByteData();
    }
}
